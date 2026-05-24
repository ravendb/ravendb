using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using static Raven.Server.ServerWide.Backups.ServerBackupRunner;

namespace Raven.Server.ServerWide.Backups
{
    /// <summary>
    /// Implements <see cref="ITombstoneAware"/> for the centralized backup runner, allowing the
    /// tombstone cleaner to determine the minimum etag that any backup task on this database has
    /// processed. One instance is created per database and registered with the tombstone cleaner at
    /// database startup. Queries the centralized runner's state rather than any per-database runner.
    /// </summary>
    public class BackupTombstoneCleanerInfo : ITombstoneAware
    {
        public BackupTombstoneCleanerInfo(ServerStore serverStore, DocumentDatabase database)
        {
            _serverStore = serverStore;
            _database = database;
        }

        private readonly ServerStore _serverStore;
        private readonly DocumentDatabase _database;
        public string TombstoneCleanerIdentifier => "Periodic Backup";
        /// <summary>
        /// Returns the minimum etag processed by any active backup task for the given tombstone type,
        /// keyed by collection name. The tombstone cleaner uses this to avoid deleting tombstones that
        /// a pending backup still needs. Returns null when all tasks have processed everything (no constraint).
        /// </summary>
        public Dictionary<string, long> GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType tombstoneType, Dictionary<string, LastTombstoneInfo> lastProcessedTombstonesInfo = null)
        {
            string collection = tombstoneType switch
            {
                ITombstoneAware.TombstoneType.Documents => Constants.Documents.Collections.AllDocumentsCollection,
                ITombstoneAware.TombstoneType.TimeSeries => Constants.TimeSeries.All,
                ITombstoneAware.TombstoneType.Counters => Constants.Counters.All,
                _ => throw new NotSupportedException($"Tombstone type '{tombstoneType}' is not supported.")
            };

            var minLastEtag = GetMinimalEtagForTombstoneCleanupForBackup(_database, lastProcessedTombstonesInfo, collection);

            if (minLastEtag == long.MaxValue)
                return null;

            var result = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase) { { collection, minLastEtag } };

            return result;
        }
        
        /// <summary>
        /// Returns disabled backup tasks as tombstone deletion blockers so the cleaner knows not to
        /// count them in the "safe to delete" calculation. Disabled tasks never advance their etag,
        /// so they would otherwise block tombstone cleanup indefinitely; callers handle this specially.
        /// </summary>
        public Dictionary<TombstoneDeletionBlockageSource, HashSet<string>> GetDisabledSubscribersCollections(HashSet<string> tombstoneCollections)
        {
            var dict = new Dictionary<TombstoneDeletionBlockageSource, HashSet<string>>();

            var data = _serverStore.BackupRunner.BackupsPerDatabasePerTaskId.TryGetValue(_database.Name, out ConcurrentDictionary<long, DatabaseBackupState> databaseBackupStates);
            if (data == false)
                return dict;

            foreach (var config in databaseBackupStates.Values.ToList().Select(x => x.Configuration).Where(config => config.Disabled))
            {
                var source = new TombstoneDeletionBlockageSource(ITombstoneAware.TombstoneDeletionBlockerType.Backup, config.Name, config.TaskId);
                dict[source] = tombstoneCollections;
            }

            return dict;
        }

        /// <summary>
        /// Computes the minimum etag across all backup tasks for this database, reading persisted status
        /// from the cluster value store. Returns 0 if this node might run a first-ever backup (to block
        /// all cleanup), or <see cref="long.MaxValue"/> when no tasks have any status to constrain cleanup.
        /// </summary>
        private long GetMinimalEtagForTombstoneCleanupForBackup(DocumentDatabase database, Dictionary<string, LastTombstoneInfo> lastProcessedTombstonesInfo = null, string collection = null)
        {
            var min = long.MaxValue;

            using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var record = _serverStore.Cluster.ReadRawDatabaseRecord(context, database.Name);
                foreach (var taskId in record.PeriodicBackupsTaskIds)
                {
                    var config = record.GetPeriodicBackupConfiguration(taskId);

                    var localStatus = BackupStatusStorage.GetBackupStatus(context, database.Name, taskId);
                    if (localStatus == null)
                    {
                        var responsibleNode = BackupUtils.GetResponsibleNodeTag(_serverStore, database.Name, taskId);
                        if (responsibleNode == null || responsibleNode == _serverStore.NodeTag)
                        {
                            // the first backup might run on this node, don't delete anything until then
                            // if there is no status for this, we don't need to take into account tombstones
                            lastProcessedTombstonesInfo?.Add($"{config.Name}/{collection}", new LastTombstoneInfo(config.Name, collection, 0, ITombstoneAware.TombstoneDeletionBlockerType.Backup));
                            return 0;
                        }

                        // we never ran the backup and aren't in the middle of it either.
                        // our next backup on this node is going to be full (first backup), so we can delete tombstones
                        continue;
                    }

                    var etag = ChangeVectorUtils.GetEtagById(localStatus.LastDatabaseChangeVector, database.DbBase64Id);
                    lastProcessedTombstonesInfo?.Add($"{config.Name}/{collection}", new LastTombstoneInfo(config.Name, collection, etag, ITombstoneAware.TombstoneDeletionBlockerType.Backup));
                    min = Math.Min(etag, min);
                }

                return min;
            }
        }
    }
}
