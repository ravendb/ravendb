using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils
{
    public class BackupHistoryWriter
    {
        public const string IdPropertyName = "Id";
        public const string TaskNamePropertyName = "TaskName";

        private readonly string _currentNodeTag;
        private readonly string _requestedNodeTag;
        private readonly long? _requestedTaskId;
        private readonly Dictionary<(string NodeTag, long TaskId), List<BlittableJsonReaderObject>> _clusterStorageEntries = new();
        private readonly Dictionary<long, List<BlittableJsonReaderObject>> _temporaryStorageEntries = new();
        private readonly HashSet<DateTime> _clusterEntriesCreationDateTimes = new();
        private bool _isFirst = true;

        public BackupHistoryWriter(string requestedNodeTag, long? requestedTaskId, string currentNodeTag)
        {
            _requestedNodeTag = requestedNodeTag;
            _requestedTaskId = requestedTaskId;
            _currentNodeTag = currentNodeTag;
        }

        public void GetBackupHistoryEntriesFromCluster(DocumentDatabase database, ServerStore serverStore, TransactionOperationContext context)
        {
            var prefix = BackupHistoryEntry.GenerateItemPrefix(database.Name);
            var itemsFromClusterStorage = serverStore.Cluster.ItemsStartingWith(context, prefix, 0, long.MaxValue);

            foreach ((string _, long _, BlittableJsonReaderObject itemValue) in itemsFromClusterStorage)
            {
                itemValue.TryGet(nameof(BackupHistory), out BlittableJsonReaderArray entries);
                foreach (BlittableJsonReaderObject entry in entries)
                {
                    entry.TryGet(nameof(BackupHistory.FullBackup), out BlittableJsonReaderObject fullBackup);
                    fullBackup.TryGet(nameof(BackupHistoryEntry.TaskId), out long taskId);
                    fullBackup.TryGet(nameof(BackupHistoryEntry.NodeTag), out string nodeTag);
                    fullBackup.TryGet(nameof(BackupHistoryEntry.CreatedAt), out DateTime fullBackupCreatedAt);
                    var taskName = database.ReadDatabaseRecord()?.PeriodicBackups.SingleOrDefault(x => x.TaskId == taskId)?.Name;

                    var entryToAdd = AddIdentifierAndTaskNamePropertiesAndUpdate(entry, context, fullBackupCreatedAt, taskName);

                    if (_clusterStorageEntries.TryGetValue((nodeTag, taskId), out List<BlittableJsonReaderObject> perNodeTaskIdEntries))
                        perNodeTaskIdEntries.Add(entryToAdd);
                    else
                        _clusterStorageEntries.Add((nodeTag, taskId), new List<BlittableJsonReaderObject> { entryToAdd });

                    // If the cluster is unstable, a backup entry could exist in both the cluster and temporary storage simultaneously.
                    // It's crucial to ensure entry uniqueness to prevent duplicates in the response.
                    if (nodeTag != _currentNodeTag)
                        return;

                    _clusterEntriesCreationDateTimes.Add(fullBackupCreatedAt);

                    entryToAdd.TryGet(nameof(BackupHistory.IncrementalBackups), out BlittableJsonReaderArray increments);
                    foreach (BlittableJsonReaderObject increment in increments)
                    {
                        increment.TryGet(nameof(BackupHistoryEntry.CreatedAt), out DateTime createdAt);
                        _clusterEntriesCreationDateTimes.Add(createdAt);
                    }
                }
            }
        }
        
        public void GetBackupHistoryEntriesFromTemporaryStorage(DocumentDatabase database, TransactionOperationContext serverStoreContext,
            TransactionOperationContext context)
        {
            var entriesFromTemporaryStorage = database.ConfigurationStorage.BackupHistoryStorage.ReadItems(context, BackupHistoryItemType.HistoryEntry);

            if (string.IsNullOrWhiteSpace(_requestedNodeTag) == false && _requestedNodeTag != _currentNodeTag || entriesFromTemporaryStorage == null)
                return;

            foreach (BlittableJsonReaderObject entry in entriesFromTemporaryStorage.Values)
            {
                entry.TryGet(nameof(BackupHistoryEntry.TaskId), out long taskId);

                if (_requestedTaskId.HasValue && _requestedTaskId.Value != taskId)
                    continue;

                var taskName = database.ReadDatabaseRecord()?.PeriodicBackups.SingleOrDefault(x => x.TaskId == taskId)?.Name;

                entry.TryGet(nameof(BackupHistoryEntry.CreatedAt), out DateTime createdAt);
                var entryToAdd = AddIdentifierAndTaskNamePropertiesAndUpdate(entry, serverStoreContext, createdAt, taskName);

                if (_temporaryStorageEntries.TryGetValue(taskId, out List<BlittableJsonReaderObject> entries))
                    entries.Add(entryToAdd);
                else
                    _temporaryStorageEntries.Add(taskId, new List<BlittableJsonReaderObject> { entryToAdd });
            }
        }

        public void ProcessClusterStorageEntries(AsyncBlittableJsonTextWriter writer, TransactionOperationContext context)
        {
            foreach (var entry in _clusterStorageEntries.Where(x => ShouldProcessEntry(x.Key.NodeTag, x.Key.TaskId)))
                for (int i = 0; i < entry.Value.Count; i++)
                {
                    var entryCs = entry.Value[i];

                    if (ShouldWriteObject(entry.Key.NodeTag, entry.Key.TaskId, entry.Value.Count, i))
                    {
                        WriteObject(writer, entryCs);
                        continue;
                    }

                    // This is the final entry from Cluster Storage for this TaskID.
                    // There might be temporary storage entries that could be added to the increments of this backup history entry.
                    _temporaryStorageEntries.TryGetValue(entry.Key.TaskId, out var etsWithSpecificTaskId);
                    AppendLastEntriesAndWrite(writer, context, etsWithSpecificTaskId, entryCs);
                }
        }

        private bool ShouldProcessEntry(string nodeTag, long taskId)
        {
            var isRequestedTask = _requestedTaskId.HasValue == false || _requestedTaskId.Value == taskId;
            var isRequestedNode = string.IsNullOrWhiteSpace(_requestedNodeTag) || _requestedNodeTag == nodeTag;
            return isRequestedTask && isRequestedNode;
        }
        
        private bool ShouldWriteObject(string nodeTag, long taskId, int numberOfEntries, int iterator)
        {
            // The current cluster storage entry points to a backup history entry from a different node.
            // We won't have any corresponding entries in temporary storage for this. Let's record it.
            if (nodeTag != _currentNodeTag)
                return true;

            // In case of cluster issues, we should retrieve entries from temporary storage.
            // If there are no entries for this TaskID in temporary storage, let's record the current entry.
            if (_temporaryStorageEntries.ContainsKey(taskId) == false)
                return true;

            // All entries from Cluster Storage will be recorded, except for the final one.
            return iterator != numberOfEntries - 1;
        }

        public void ProcessTemporaryStorageEntries(AsyncBlittableJsonTextWriter writer, TransactionOperationContext context)
        {
            // If there are entries in temporary storage that aren't linked to any TaskID, they should also be included in the response.
            if (string.IsNullOrWhiteSpace(_requestedNodeTag) == false && _requestedNodeTag != _currentNodeTag)
                return;

            var oddments = _temporaryStorageEntries
                .Where(odd => _requestedTaskId.HasValue == false || _requestedTaskId.Value == odd.Key);

            foreach (var oddment in oddments)
                AppendLastEntriesAndWrite(writer, context, oddment.Value);
        }

        private void WriteObject(AsyncBlittableJsonTextWriter writer, BlittableJsonReaderObject obj)
        {
            if (_isFirst == false)
                writer.WriteComma();

            writer.WriteObject(obj);
            _isFirst = false;
        }

        private void AppendLastEntriesAndWrite(AsyncBlittableJsonTextWriter writer, TransactionOperationContext context,
            IReadOnlyList<BlittableJsonReaderObject> entriesToAdd, BlittableJsonReaderObject lastEntry = null)
        {
            foreach (BlittableJsonReaderObject entryToAdd in entriesToAdd)
            {
                entryToAdd.TryGet(nameof(BackupHistoryEntry.CreatedAt), out DateTime createdAt);

                if (_clusterEntriesCreationDateTimes.Contains(createdAt)) 
                    continue;

                entryToAdd.TryGet(nameof(BackupHistoryEntry.IsFull), out bool isFull);
                if (isFull)
                {
                    if (lastEntry != null) WriteObject(writer, lastEntry);

                    var newEntry = new DynamicJsonValue
                    {
                        [nameof(BackupHistory.FullBackup)] = entryToAdd, 
                        [nameof(BackupHistory.IncrementalBackups)] = new DynamicJsonArray()
                    };

                    lastEntry = context.ReadObject(newEntry, null);
                    continue;
                }

                Debug.Assert(lastEntry != null);

                lastEntry.TryGet(nameof(BackupHistory.FullBackup), out BlittableJsonReaderObject lastEntryFullBackup);
                lastEntryFullBackup.TryGet(nameof(BackupHistoryEntry.LastFullBackup), out DateTime? lastEntryLastFullBackup);
                entryToAdd.TryGet(nameof(BackupHistoryEntry.LastFullBackup), out DateTime? entryToAddLastFullBackup);

                if (entryToAddLastFullBackup != lastEntryLastFullBackup) continue;

                lastEntry.TryGet(nameof(BackupHistory.IncrementalBackups), out BlittableJsonReaderArray increments);
                increments.Modifications ??= new DynamicJsonArray();
                increments.Modifications.Add(entryToAdd);

                lastEntry = context.ReadObject(lastEntry, null);
            }

            WriteObject(writer, lastEntry);

            entriesToAdd[0].TryGet(nameof(BackupHistoryEntry.TaskId), out long taskId);
            _temporaryStorageEntries.Remove(taskId);
        }

        private static BlittableJsonReaderObject AddIdentifierAndTaskNamePropertiesAndUpdate(BlittableJsonReaderObject obj, TransactionOperationContext context,
            DateTime createdAt, string taskName)
        {
            obj.TryGet(nameof(BackupHistory.FullBackup), out BlittableJsonReaderObject fullBackup);
            AddIdentifierAndTaskNameProperties(fullBackup, createdAt, taskName);

            obj.TryGet(nameof(BackupHistory.IncrementalBackups), out BlittableJsonReaderArray increments);
            foreach (var increment in increments)
            {
                var bjroIncrement = (BlittableJsonReaderObject)increment;
                bjroIncrement.TryGet(nameof(BackupHistoryEntry.CreatedAt), out DateTime incrementCreatedAt);

                AddIdentifierAndTaskNameProperties(bjroIncrement, incrementCreatedAt, taskName);
            }

            return context.ReadObject(obj, "updated");
        }

        private static void AddIdentifierAndTaskNameProperties(BlittableJsonReaderObject obj, DateTime createdAt, string taskName)
        {
            obj.Modifications = new DynamicJsonValue
            {
                [IdPropertyName] = createdAt.Ticks.ToString(),
                [TaskNamePropertyName] = taskName
            };
        }
    }
}

