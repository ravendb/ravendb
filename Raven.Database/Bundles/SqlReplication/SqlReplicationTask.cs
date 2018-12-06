// -----------------------------------------------------------------------
//  <copyright file="SqlReplicationTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Database.Prefetching;
using Raven.Database.Storage;
using Raven.Imports.metrics;
using Raven.Json.Linq;
using Sparrow.Collections;

namespace Raven.Database.Bundles.SqlReplication
{
    [InheritedExport(typeof(IStartupTask))]
    [ExportMetadata("Bundle", "sqlReplication")]
    [CLSCompliant(false)]
    public class SqlReplicationTask : IStartupTask, IDisposable
    {
        private const int MaxNumberOfDeletionsToReplicate = 1024;

        private const int MaxNumberOfChangesToReplicate = 4096;
        private const int MaxBatchSizeToReturnToNormal = 256;

        private int changesBatchSize = MaxNumberOfChangesToReplicate;

        private volatile bool shouldPause;

        public bool IsRunning { get; private set; }

        private class ReplicatedDoc
        {
            public RavenJObject Document;
            public Etag Etag;
            public int SerializedSizeOnDisk;
            public string Key;
        }

        public const string RavenSqlReplicationStatus = "Raven/SqlReplication/Status";

        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        public event Action<int> AfterReplicationCompleted = delegate { };
        readonly Metrics sqlReplicationMetrics = new Metrics();

        public DocumentDatabase Database { get; set; }

        private List<SqlReplicationConfig> replicationConfigs;
        private readonly ConcurrentDictionary<string, SqlReplicationStatistics> statistics = new ConcurrentDictionary<string, SqlReplicationStatistics>(StringComparer.InvariantCultureIgnoreCase);
        public ConcurrentDictionary<string, SqlReplicationStatistics> Statistics
        {
            get { return statistics; }
        }

        public ConcurrentDictionary<string, bool> ResetRequested
        {
            get { return resetRequested; }
        }
        public readonly ConcurrentDictionary<string, SqlReplicationMetricsCountersManager> SqlReplicationMetricsCounters =
            new ConcurrentDictionary<string, SqlReplicationMetricsCountersManager>();

        private readonly ConcurrentSet<PrefetchingBehavior> prefetchingBehaviors = new ConcurrentSet<PrefetchingBehavior>();

        private readonly ConcurrentDictionary<string, bool> resetRequested = new ConcurrentDictionary<string, bool>();

        public void Execute(DocumentDatabase database)
        {
            Database = database;
            Database.Notifications.OnDocumentChange += (sender, notification, metadata) =>
            {
                if (notification.Id == null)
                    return;

                if (metadata == null)
                    return; // this is a delete being made on an already deleted document

                if (notification.Type == DocumentChangeTypes.Delete)
                {
                    using(Database.DocumentLock.Lock())
                    {
                        RecordDelete(notification.Id, metadata);
                    }
                }

                if (!notification.Id.StartsWith("Raven/SqlReplication/Configuration/", StringComparison.InvariantCultureIgnoreCase)
                    && string.Compare(notification.Id, "Raven/SqlReplication/Connections", StringComparison.InvariantCultureIgnoreCase) != 0)
                    return;

                replicationConfigs = null;
                if (Log.IsDebugEnabled)
                    Log.Debug(() => "Sql Replication configuration was changed.");
            };

            GetReplicationStatus();

            var task = Task.Factory.StartNew(() =>
            {
                using (LogContext.WithResource(database.Name))
                {
                    try
                    {
                        BackgroundSqlReplication();
                    }
                    catch (Exception e)
                    {
                        Log.ErrorException("Fatal failure when replicating to SQL. All SQL Replication activity STOPPED", e);
                    }
                }
            }, TaskCreationOptions.LongRunning);
            database.ExtensionsState.GetOrAdd(typeof(SqlReplicationTask).FullName, k => new DisposableAction(task.Wait));
        }

        public void Pause()
        {
            shouldPause = true;
        }

        public void Continue()
        {
            shouldPause = false;
        }

        private void RecordDelete(string id, RavenJObject metadata)
        {
            Etag etag = null;
            Database.TransactionalStorage.Batch(accessor =>
            {
                bool hasChanges = false;
                foreach (var config in GetConfiguredReplicationDestinations())
                {
                    if (string.Equals(config.RavenEntityName, metadata.Value<string>(Constants.RavenEntityName), StringComparison.InvariantCultureIgnoreCase) == false)
                        continue;

                    hasChanges = true;
                    string sqlReplicationName = GetSqlReplicationDeletionName(config);
                    etag = accessor.Lists.Set(sqlReplicationName, id, metadata, UuidType.Documents);
                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug(() => $"For sql replication replication {sqlReplicationName} recorded a deleted document, under etag: {etag}, document etag: { metadata.Value<string>(Constants.MetadataEtagField)} id: {id}");
                    }
                }
                if (hasChanges)
                    Database.WorkContext.ShouldNotifyAboutWork(() => "recorded a deleted document " + id);
            });
            
                
        }

        private SqlReplicationStatus GetReplicationStatus()
        {
            var jsonDocument = Database.Documents.Get(RavenSqlReplicationStatus, null);
            return jsonDocument == null
                                    ? new SqlReplicationStatus()
                                    : jsonDocument.DataAsJson.JsonDeserialization<SqlReplicationStatus>();
        }

        public SqlReplicationMetricsCountersManager GetSqlReplicationMetricsManager(SqlReplicationConfig cfg)
        {
            return SqlReplicationMetricsCounters.GetOrAdd(cfg.Name,
                s => new SqlReplicationMetricsCountersManager(sqlReplicationMetrics, cfg)
                );
        }

        private bool IsHotSpare()
        {
            if (Database.RequestManager == null) return false;
            return Database.RequestManager.IsInHotSpareMode;
        }

        private void BackgroundSqlReplication()
        {
            int workCounter = 0;
            while (Database.WorkContext.DoWork)
            {
                IsRunning = !IsHotSpare() && !shouldPause;

                if (!IsRunning)
                {
                    Database.WorkContext.WaitForWork(TimeSpan.FromMinutes(10), ref workCounter, "Sql Replication");

                    continue;
                }

                var config = GetConfiguredReplicationDestinations();
                if (config.Count == 0)
                {
                    Database.WorkContext.WaitForWork(TimeSpan.FromMinutes(10), ref workCounter, "Sql Replication");
                    continue;
                }
                var localReplicationStatus = GetReplicationStatus();

                // remove all last replicated statuses which are not in the config
                UpdateLastReplicatedStatus(localReplicationStatus, config);

                var relevantConfigs = config.Where(x =>
                {
                    if (x.Disabled)
                        return false;
                    var sqlReplicationStatistics = statistics.GetOrDefault(x.Name);
                    if (sqlReplicationStatistics == null)
                        return true;
                    return SystemTime.UtcNow >= sqlReplicationStatistics.SuspendUntil;
                }) // have error or the timeout expired
                .ToList();

                var configGroups = SqlReplicationClassifier.GroupConfigs(relevantConfigs, c => GetLastEtagFor(localReplicationStatus, c));

                if (configGroups.Count == 0)
                {
                    Database.WorkContext.WaitForWork(TimeSpan.FromMinutes(10), ref workCounter, "Sql Replication");
                    continue;
                }


                if (Log.IsDebugEnabled)
                {
                    Log.Debug($@"SqlReplication grouping: {Environment.NewLine} {string.Join(";",configGroups.Select(x => $"{x.Key}: ({string.Join(",", x.Value.Select(y => y.Name))}){Environment.NewLine}"))}");
                }


                var usedPrefetchers = new ConcurrentSet<PrefetchingBehavior>();

                var groupedConfigs = configGroups
                    .Select(x =>
                    {
                        var result = new SqlConfigGroup
                        {
                            LastReplicatedEtag = x.Key,
                            ConfigsToWorkOn = x.Value
                        };

                        SetPrefetcherForIndexingGroup(result, usedPrefetchers);

                        return result;
                    })
                    .ToList();

                var successes = new ConcurrentQueue<Tuple<SqlReplicationConfigWithLastReplicatedEtag, Etag>>();
                var waitForWork = new bool[groupedConfigs.Count];
                try
                {
                    BackgroundTaskExecuter.Instance.ExecuteAll(Database.WorkContext, groupedConfigs, (sqlConfigGroup, i) =>
                    {
                        Database.WorkContext.CancellationToken.ThrowIfCancellationRequested();
                        
                        var prefetchingBehavior = sqlConfigGroup.PrefetchingBehavior;
                        var configsToWorkOn = sqlConfigGroup.ConfigsToWorkOn.ToList();//create clone

                        List<JsonDocument> documents;
                        using (prefetchingBehavior.DocumentBatchFrom(sqlConfigGroup.LastReplicatedEtag, changesBatchSize, out documents))
                        {
                            Etag lastBatchEtag = null;
                            if (documents.Count != 0)
                                lastBatchEtag = documents[documents.Count - 1].Etag;

                            var replicationDuration = Stopwatch.StartNew();
                            documents.RemoveAll(x => x.Key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase)); // we ignore system documents here

                            documents.RemoveAll(x => prefetchingBehavior.FilterDocuments(x) == false);

                            var deletedDocsByConfig = new Dictionary<SqlReplicationConfig, List<ListItem>>();

                            foreach (var configToWorkOn in configsToWorkOn)
                            {
                                var cfg = configToWorkOn;

                                Database.TransactionalStorage.Batch(accessor =>
                                {
                                    List<ListItem> deletedDocs = accessor.Lists.Read(GetSqlReplicationDeletionName(cfg),
                                            cfg.LastReplicatedEtag,
                                            lastBatchEtag,
                                            MaxNumberOfDeletionsToReplicate + 1)
                                        .ToList();

                                    if (deletedDocs.Count > MaxNumberOfDeletionsToReplicate-1 && 
                                        lastBatchEtag != null)
                                    {


                                        // we have more docs in range then we allow for, so we need to trim
                                        // the documents to the last delete here

                                        var lastDeleteEtag = deletedDocs[deletedDocs.Count - 1].Etag;
                                        var docsCountBeforeRemovalOfDocumentsAfterLastDeletedDocToSend = documents.Count;
                                        documents.RemoveAll(x => x.Etag.CompareTo(lastBatchEtag) >= 0);
                                        if (lastBatchEtag.CompareTo(lastDeleteEtag) > 0)
                                        {
                                            if (Log.IsDebugEnabled)
                                            {
                                                var sb = new StringBuilder();
                                                sb.Append($"For sqlReplications {cfg.Name} there were more deleted docs then {MaxNumberOfDeletionsToReplicate - 1}. ");
                                                sb.Append($"In order to maintain replication order, lastBatchEtag was modified from {lastBatchEtag} to {lastDeleteEtag}. ");

                                                if (docsCountBeforeRemovalOfDocumentsAfterLastDeletedDocToSend > documents.Count)
                                                {
                                                    sb.Append($"{docsCountBeforeRemovalOfDocumentsAfterLastDeletedDocToSend - documents.Count} won't be sent");
                                                }

                                                Log.Debug(sb.ToString());
                                            }
                                            lastBatchEtag = lastDeleteEtag;
                                        }
                                        else if (Log.IsDebugEnabled)
                                        {
                                            var sb = new StringBuilder();
                                            sb.Append($"For sqlReplications {cfg.Name} there were more deleted docs then {MaxNumberOfDeletionsToReplicate - 1}. ");
                                            if (docsCountBeforeRemovalOfDocumentsAfterLastDeletedDocToSend > documents.Count)
                                            {
                                                sb.Append($"{docsCountBeforeRemovalOfDocumentsAfterLastDeletedDocToSend - documents.Count} won't be sent");
                                            }
                                            
                                            Log.Debug(sb.ToString());                                                
                                        }
                                    }

                                    if (Log.IsDebugEnabled)
                                    {
                                        var docIDsToDelete = string.Join(" , ", deletedDocs.Select(x => x.Key));
                                        Log.Debug($"For sqlReplication {cfg.Name} for Documents found to delete from etag {cfg.LastReplicatedEtag } to etag {lastBatchEtag} in sql replication {cfg.Name}: {docIDsToDelete}");
                                    }
                                    deletedDocsByConfig[cfg] = deletedDocs;
                                });
                            }

                            // No documents AND there aren't any deletes to replicate
                            if (documents.Count == 0)
                            {
                                foreach (var item in deletedDocsByConfig)
                                {
                                    var cfg = configsToWorkOn.Single(x => x.Name == item.Key.Name);
                                    if (item.Value.Count == 0)
                                    {                                        
                                        waitForWork[i] = true;

                                        if (Log.IsDebugEnabled)                                                                                   
                                            Log.Debug($"For sqlReplication {cfg.Name}, there are no documents or deletes to be sent, there will be no attempt to replicate. ");                                                                                       

                                        configsToWorkOn.Remove(cfg);
                                    }
                                }
                                if (configsToWorkOn.Count == 0)
                                {
                                    if (Log.IsDebugEnabled)
                                        Log.Debug($"No documents or deletes were found in current batch, all sqlReplication group activated ");

                                    return;
                                }
                            }

                            var itemsToReplicate = documents.Select(x =>
                            {
                                JsonDocument.EnsureIdInMetadata(x);
                                var doc = x.ToJson();
                                doc[Constants.DocumentIdFieldName] = x.Key;

                                return new ReplicatedDoc
                                {
                                    Document = doc,
                                    Etag = x.Etag,
                                    Key = x.Key,
                                    SerializedSizeOnDisk = x.SerializedSizeOnDisk
                                };
                            }).ToList();

                            try
                            {
                                BackgroundTaskExecuter.Instance.ExecuteAllInterleaved(Database.WorkContext, configsToWorkOn, replicationConfig =>
                                {
                                    try
                                    {
                                        var startTime = SystemTime.UtcNow;
                                        var spRepTime = new Stopwatch();
                                        spRepTime.Start();
                                        var lastReplicatedEtag = replicationConfig.LastReplicatedEtag;
                                        
                                        var deletedDocs = deletedDocsByConfig[replicationConfig];
                                        var docsToReplicate = itemsToReplicate
                                            .Where(x => lastReplicatedEtag.CompareTo(x.Etag) < 0); // haven't replicate the etag yet

                                        if (deletedDocs.Count >= MaxNumberOfDeletionsToReplicate + 1)
                                        {
                                            if (Log.IsDebugEnabled)
                                            {
                                                Log.Debug($"Found too much documents to replicate in sql replication task: {replicationConfig.Name}");
                                            }
                                            docsToReplicate = docsToReplicate.Where(x => EtagUtil.IsGreaterThan(x.Etag, deletedDocs[deletedDocs.Count - 1].Etag) == false);
                                        }

                                        var docsToReplicateAsList = docsToReplicate.ToList();

                                        var currentLatestEtag = HandleDeletesAndChangesMerging(deletedDocs, docsToReplicateAsList, replicationConfig);
                                        if (currentLatestEtag == null && itemsToReplicate.Count > 0 && docsToReplicateAsList.Count == 0)
                                        {
                                            if (Log.IsDebugEnabled)
                                            {
                                                Log.Debug($"for sqlReplication {replicationConfig.Name} currentLatestEtag could not be found from deletedDocs and docsToReplicateAsList, bumping currentLastEtag to {lastBatchEtag}");
                                            }
                                            currentLatestEtag = lastBatchEtag;
                                        }

                                        int countOfReplicatedItems = 0;
                                        if (ReplicateDeletionsToDestination(replicationConfig, deletedDocs) &&
                                            ReplicateChangesToDestination(replicationConfig, docsToReplicateAsList, out countOfReplicatedItems))
                                        {
                                            if (deletedDocs.Count > 0)
                                            {
                                                if (Log.IsDebugEnabled)
                                                {
                                                    Log.Debug($"For sqlReplication {replicationConfig.Name} Removing all replicated deletes prior to etag {deletedDocs[deletedDocs.Count - 1].Etag}");
                                                }

                                                Database.TransactionalStorage.Batch(accessor =>
                                                    accessor.Lists.RemoveAllBefore(GetSqlReplicationDeletionName(replicationConfig), deletedDocs[deletedDocs.Count - 1].Etag));
                                            }

                                            if (Log.IsDebugEnabled)
                                            {
                                                Log.Debug($"For sqlReplication {replicationConfig.Name} updating successes with etag {currentLatestEtag}");
                                            }
                                            successes.Enqueue(Tuple.Create(replicationConfig, currentLatestEtag));

                                            changesBatchSize = Math.Min(changesBatchSize * 2, MaxBatchSizeToReturnToNormal);

                                            //if we successfully replicated multiple times, return to normal batch size
                                            if (changesBatchSize >= MaxBatchSizeToReturnToNormal)
                                                changesBatchSize = MaxNumberOfChangesToReplicate;
                                        }
                                        else
                                        {
                                            changesBatchSize = 1; //failed replicating deletes or changes, so next time try small batch
                                        }

                                        spRepTime.Stop();
                                        var elapsedMicroseconds = (long) (spRepTime.ElapsedTicks * SystemTime.MicroSecPerTick);

                                        var sqlReplicationMetricsCounters = GetSqlReplicationMetricsManager(replicationConfig);
                                        sqlReplicationMetricsCounters.SqlReplicationBatchSizeMeter.Mark(countOfReplicatedItems);
                                        sqlReplicationMetricsCounters.SqlReplicationBatchSizeHistogram.Update(countOfReplicatedItems);
                                        sqlReplicationMetricsCounters.SqlReplicationDurationHistogram.Update(elapsedMicroseconds);

                                        UpdateReplicationPerformance(replicationConfig, startTime, spRepTime.Elapsed, docsToReplicateAsList.Count);
                                    }
                                    catch (Exception e)
                                    {
                                        Log.WarnException("Error while replication to SQL destination: " + replicationConfig.Name, e);
                                        Database.AddAlert(new Alert
                                        {
                                            AlertLevel = AlertLevel.Error,
                                            CreatedAt = SystemTime.UtcNow,
                                            Exception = e.ToString(),
                                            Title = "Sql Replication failure to replication",
                                            Message = "Sql Replication could not replicate to " + replicationConfig.Name,
                                            UniqueKey = "Sql Replication could not replicate to " + replicationConfig.Name
                                        });
                                        changesBatchSize = 1;
                                    }
                                });
                            }
                            finally
                            {
                                prefetchingBehavior.CleanupDocuments(lastBatchEtag);
                                prefetchingBehavior.UpdateAutoThrottler(documents, replicationDuration.Elapsed);
                            }
                        }
                    });


                    if (Log.IsDebugEnabled)
                        Log.Debug($"Successes count is: {successes.Count}, waitForWork count is: {waitForWork.Count(x=>x)} out of {successes.Count}");

                    if (successes.Count == 0)
                    {
                        if (waitForWork.All(x => x))
                            Database.WorkContext.WaitForWork(TimeSpan.FromMinutes(10), ref workCounter, "Sql Replication");

                        continue;
                    }

                    foreach (var t in successes)
                    {
                        var cfg = t.Item1;
                        var currentLatestEtag = t.Item2;
                        //If a reset was requested we don't want to update the last replicated etag.
                        //If we do register the success the reset will become a noop.
                        bool isReset;
                        if (ResetRequested.TryGetValue(t.Item1.Name, out isReset) && isReset)
                            continue;

                        var destEtag = localReplicationStatus.LastReplicatedEtags.FirstOrDefault(x => string.Equals(x.Name, cfg.Name, StringComparison.InvariantCultureIgnoreCase));
                        if (destEtag == null)
                        {
                            localReplicationStatus.LastReplicatedEtags.Add(new LastReplicatedEtag
                            {
                                Name = cfg.Name,
                                LastDocEtag = currentLatestEtag ?? Etag.Empty
                            });
                        }
                        else
                        {
                            var lastDocEtag = destEtag.LastDocEtag;
                            if (currentLatestEtag != null && EtagUtil.IsGreaterThan(currentLatestEtag, lastDocEtag))
                            {
                                if (Log.IsDebugEnabled)
                                    Log.Debug($"Switched LastDocEtag ({destEtag.LastDocEtag})to a higher one ({currentLatestEtag}) in {cfg.Name}");

                                lastDocEtag = currentLatestEtag;
                            }
                            destEtag.LastDocEtag = lastDocEtag;
                        }
                    }
                    //We are done recording success for this batch so we can clear the reset dictionary
                    ResetRequested.Clear();
                    SaveNewReplicationStatus(localReplicationStatus);
                }              
                finally
                {
                    AfterReplicationCompleted(successes.Count);
                    RemoveUnusedPrefetchers(usedPrefetchers);
                }
            }
        }

        private void UpdateLastReplicatedStatus(SqlReplicationStatus localReplicationStatus, List<SqlReplicationConfig> config)
        {
            var lastReplicatedToDelete = new List<LastReplicatedEtag>();
            foreach (var lastReplicated in localReplicationStatus.LastReplicatedEtags)
            {
                if (config.Exists(x => x.Name.Equals(lastReplicated.Name, StringComparison.InvariantCultureIgnoreCase)) == false)
                {
                    lastReplicatedToDelete.Add(lastReplicated);
                }
            }

            if (lastReplicatedToDelete.Count == 0)
                return; // nothing to do

            foreach (var status in lastReplicatedToDelete)
            {
                localReplicationStatus.LastReplicatedEtags.Remove(status);
            }

            SaveNewReplicationStatus(localReplicationStatus);
        }

        private void SetPrefetcherForIndexingGroup(SqlConfigGroup sqlConfig, ConcurrentSet<PrefetchingBehavior> usedPrefetchers)
        {
            var entityNames = new HashSet<string>(sqlConfig.ConfigsToWorkOn.Select(x => x.RavenEntityName), StringComparer.OrdinalIgnoreCase);
            sqlConfig.PrefetchingBehavior = TryGetPrefetcherFor(sqlConfig.LastReplicatedEtag, usedPrefetchers, entityNames) ??
                                            GetPrefetcherFor(sqlConfig.LastReplicatedEtag, usedPrefetchers, entityNames);

            sqlConfig.PrefetchingBehavior.SetEntityNames(entityNames);

            sqlConfig.PrefetchingBehavior.AdditionalInfo =
                $"For SQL config group: [Configs: {string.Join(", ", sqlConfig.ConfigsToWorkOn.Select(y => y.Name))}, " +
                $"Last Replicated Etag: {sqlConfig.LastReplicatedEtag}], collections: {string.Join(", ", entityNames)}";
        }

        private PrefetchingBehavior TryGetPrefetcherFor(Etag fromEtag, 
            ConcurrentSet<PrefetchingBehavior> usedPrefetchers, HashSet<string> entityNames)
        {
            foreach (var prefetchingBehavior in prefetchingBehaviors)
            {
                if (prefetchingBehavior.CanUsePrefetcherToLoadFromUsingExistingData(fromEtag, entityNames) &&
                    usedPrefetchers.TryAdd(prefetchingBehavior))
                {
                    return prefetchingBehavior;
                }
            }

            return null;
        }

        private PrefetchingBehavior GetPrefetcherFor(Etag fromEtag, 
            ConcurrentSet<PrefetchingBehavior> usedPrefetchers, HashSet<string> entityNames)
        {
            foreach (var prefetchingBehavior in prefetchingBehaviors)
            {
                if (prefetchingBehavior.IsEmpty() && usedPrefetchers.TryAdd(prefetchingBehavior))
                    return prefetchingBehavior;
            }

            var newPrefetcher = Database.Prefetcher.CreatePrefetchingBehavior(PrefetchingUser.SqlReplicator, 
                null, $"SqlReplication, etags from: {fromEtag}", entityNames);

            prefetchingBehaviors.Add(newPrefetcher);
            usedPrefetchers.Add(newPrefetcher);

            return newPrefetcher;
        }

        private void RemoveUnusedPrefetchers(IEnumerable<PrefetchingBehavior> usedPrefetchingBehaviors)
        {
            var unused = prefetchingBehaviors.Except(usedPrefetchingBehaviors).ToList();

            if (unused.Count == 0)
                return;

            foreach (var unusedPrefetcher in unused)
            {
                prefetchingBehaviors.TryRemove(unusedPrefetcher);
                Database.Prefetcher.RemovePrefetchingBehavior(unusedPrefetcher);
            }
        }

        private void UpdateReplicationPerformance(SqlReplicationConfig replicationConfig, DateTime startTime, TimeSpan elapsed, int batchSize)
        {
            var performance = new SqlReplicationPerformanceStats
            {
                BatchSize = batchSize,
                Duration = elapsed,
                Started = startTime
            };

            var sqlReplicationMetricsCounters = GetSqlReplicationMetricsManager(replicationConfig);
            sqlReplicationMetricsCounters.ReplicationPerformanceStats.Enqueue(performance);
            while (sqlReplicationMetricsCounters.ReplicationPerformanceStats.Count() > 25)
            {
                SqlReplicationPerformanceStats _;
                sqlReplicationMetricsCounters.ReplicationPerformanceStats.TryDequeue(out _);
            }
        }

        private void SaveNewReplicationStatus(SqlReplicationStatus localReplicationStatus)
        {
            int retries = 5;
            while (retries > 0)
            {
                retries--;
                try
                {
                    var obj = RavenJObject.FromObject(localReplicationStatus);
                    Database.Documents.Put(RavenSqlReplicationStatus, null, obj, new RavenJObject(), null);

                    break;
                }
                catch (SynchronizationLockException)
                {
                    if (Log.IsDebugEnabled)
                        Log.Debug($"SaveNewReplicationStatus failed due to SynchronizationLockException");
                    // just ignore it, we'll save that next time
                    break;
                }
                catch (ConcurrencyException)
                {
                    if (Log.IsDebugEnabled)
                        Log.Debug($"SaveNewReplicationStatus failed due to ConcurrencyException");
                    Thread.Sleep(50);
                }
            }
        }

        private Etag HandleDeletesAndChangesMerging(List<ListItem> deletedDocs, List<ReplicatedDoc> docsToReplicate, SqlReplicationConfigWithLastReplicatedEtag replicationConfig)
        {
            // This code is O(N^2), I don't like it, but we don't have a lot of deletes, and in order for it to be really bad
            // we need a lot of deletes WITH a lot of changes at the same time
            for (int index = 0; index < deletedDocs.Count; index++)
            {
                var deletedDoc = deletedDocs[index];
                var change = docsToReplicate.FindIndex(
                    x => string.Equals(x.Key, deletedDoc.Key, StringComparison.InvariantCultureIgnoreCase));

                if (change == -1)
                    continue;

                // delete > doc
                if (deletedDoc.Etag.CompareTo(docsToReplicate[change].Etag) > 0)
                {
                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug($"In task {replicationConfig.Name} Document with ID {deletedDoc.Key} won't be sql replicated because it's registered for deletion after the modification");
                    }
                    // the delete came AFTER the doc, so we can remove the doc and just replicate the delete
                    docsToReplicate.RemoveAt(change);
                }
                else
                {
                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug($"In task {replicationConfig.Name} Deletion of document with ID {deletedDoc.Key} won't be sql replicated because it has been modified after it was registered for deletion");
                    }
                    // the delete came BEFORE the doc, so we can remove the delte and just replicate the change
                    deletedDocs.RemoveAt(index);
                    index--;
                }
            }

            Etag latest = null;
            if (deletedDocs.Count != 0)
                latest = deletedDocs[deletedDocs.Count - 1].Etag;

            if (docsToReplicate.Count != 0)
            {
                var maybeLatest = docsToReplicate[docsToReplicate.Count - 1].Etag;
                Debug.Assert(maybeLatest != null);
                if (latest == null)
                {
                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug($"In task {replicationConfig.Name} Latest Etag will be set to {maybeLatest} because there were no documents to delete");
                    }
                    return maybeLatest;
                }
                if (maybeLatest.CompareTo(latest) > 0)
                {
                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug($"In task {replicationConfig.Name} Latest Etag will be set to {maybeLatest} instead of {latest} because last modified etag is higher then last deleted one");
                    }
                    return maybeLatest;
                }                    
            }

            return latest;
        }

        private bool ReplicateDeletionsToDestination(SqlReplicationConfig cfg, IEnumerable<ListItem> deletedDocs)
        {
            var identifiers = deletedDocs.Select(x => x.Key).ToList();
            if (identifiers.Count == 0)
                return true;

            var replicationStats = statistics.GetOrAdd(cfg.Name, name => new SqlReplicationStatistics(name));
            using (var writer = new RelationalDatabaseWriter(Database, cfg, replicationStats))
            {
                foreach (var sqlReplicationTable in cfg.SqlReplicationTables)
                {
                    writer.DeleteItems(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, cfg.ParameterizeDeletesDisabled, identifiers);
                }
                writer.Commit();
                if (Log.IsDebugEnabled)
                    Log.Debug("Replicated deletes of {0} for config {1}", string.Join(", ", identifiers), cfg.Name);
            }

            return true;
        }

        private static string GetSqlReplicationDeletionName(SqlReplicationConfig replicationConfig)
        {
            return "SqlReplication/Deletions/" + replicationConfig.Name;
        }

        private bool ReplicateChangesToDestination(SqlReplicationConfig cfg, ICollection<ReplicatedDoc> docs, out int countOfReplicatedItems)
        {
            countOfReplicatedItems = 0;
            var replicationStats = statistics.GetOrAdd(cfg.Name, name => new SqlReplicationStatistics(name));
            var scriptResult = ApplyConversionScript(cfg, docs, replicationStats);
            if (scriptResult.Ids.Count == 0)
                return true;

            countOfReplicatedItems = scriptResult.Data.Sum(x => x.Value.Count);
            try
            {
                using (var writer = new RelationalDatabaseWriter(Database, cfg, replicationStats))
                {
                    if (writer.Execute(scriptResult))
                    {
                        if (Log.IsDebugEnabled)
                            Log.Debug("Replicated changes of {0} for replication {1}", string.Join(", ", docs.Select(d => d.Key)), cfg.Name);
                        replicationStats.CompleteSuccess(countOfReplicatedItems);
                    }
                    else
                    {
                        if (Log.IsDebugEnabled)
                            Log.Debug("Replicated changes (with some errors) of {0} for replication {1}", string.Join(", ", docs.Select(d => d.Key)), cfg.Name);
                        replicationStats.Success(countOfReplicatedItems);
                    }
                }
                return true;
            }
            catch (Exception e)
            {
                Log.WarnException("Failure to replicate changes to relational database for: " + cfg.Name, e);
                SqlReplicationStatistics replicationStatistics;
                DateTime newTime;
                if (statistics.TryGetValue(cfg.Name, out replicationStatistics) == false)
                {
                    newTime = SystemTime.UtcNow.AddSeconds(5);
                }
                else
                {
                    if (replicationStatistics.LastErrorTime == DateTime.MinValue)
                    {
                        newTime = SystemTime.UtcNow.AddSeconds(5);
                    }
                    else
                    {
                        // double the fallback time (but don't cross 15 minutes)
                        var totalSeconds = (SystemTime.UtcNow - replicationStatistics.LastErrorTime).TotalSeconds;
                        newTime = SystemTime.UtcNow.AddSeconds(Math.Min(60 * 15, Math.Max(5, totalSeconds * 2)));
                    }
                }
                replicationStats.RecordWriteError(e, Database, countOfReplicatedItems, newTime);
                return false;
            }
        }

        private ConversionScriptResult ApplyConversionScript(SqlReplicationConfig cfg, IEnumerable<ReplicatedDoc> docs, SqlReplicationStatistics replicationStats)
        {
            var result = new ConversionScriptResult();
            foreach (var replicatedDoc in docs)
            {
                Database.WorkContext.CancellationToken.ThrowIfCancellationRequested();
                if (string.IsNullOrEmpty(cfg.RavenEntityName) == false)
                {
                    var entityName = replicatedDoc.Document[Constants.Metadata].Value<string>(Constants.RavenEntityName);
                    if (string.Equals(cfg.RavenEntityName, entityName, StringComparison.InvariantCultureIgnoreCase) == false)
                        continue;
                }

                var patcher = new SqlReplicationScriptedJsonPatcher(Database, result, cfg, replicatedDoc.Key);
                using (var scope = new SqlReplicationScriptedJsonPatcherOperationScope(Database))
                {
                    try
                    {
                        patcher.Apply(scope, replicatedDoc.Document, new ScriptedPatchRequest { Script = cfg.Script }, replicatedDoc.SerializedSizeOnDisk);

                        if (Log.IsDebugEnabled && patcher.Debug.Count > 0)
                        {
                            Log.Debug("Debug output for doc: {0} for script {1}:\r\n.{2}", replicatedDoc.Key, cfg.Name, string.Join("\r\n", patcher.Debug));

                            patcher.Debug.Clear();
                        }

                        replicationStats.ScriptSuccess();
                    }
                    catch (ParseException e)
                    {
                        replicationStats.MarkScriptAsInvalid(Database, cfg.Script);

                        Log.WarnException("Could not parse SQL Replication script for " + cfg.Name, e);

                        return result;
                    }
                    catch (Exception diffExceptionName)
                    {
                        replicationStats.RecordScriptError(Database, diffExceptionName);
                        Log.WarnException("Could not process SQL Replication script for " + cfg.Name + ", skipping document: " + replicatedDoc.Key, diffExceptionName);
                    }
                }
            }
            return result;
        }

        private Etag GetLastEtagFor(SqlReplicationStatus replicationStatus, SqlReplicationConfig sqlReplicationConfig)
        {
            var lastEtag = Etag.Empty;
            var lastEtagHolder = replicationStatus.LastReplicatedEtags.FirstOrDefault(
                x => string.Equals(sqlReplicationConfig.Name, x.Name, StringComparison.InvariantCultureIgnoreCase));
            if (lastEtagHolder != null)
                lastEtag = lastEtagHolder.LastDocEtag;
            return lastEtag;
        }

        public RelationalDatabaseWriter.TableQuerySummary[] SimulateSqlReplicationSqlQueries(string strDocumentId, SqlReplicationConfig sqlReplication, bool performRolledbackTransaction, out Alert alert)
        {
            RelationalDatabaseWriter.TableQuerySummary[] resutls = null;

            try
            {
                var stats = new SqlReplicationStatistics(sqlReplication.Name, false);

                var jsonDocument = Database.Documents.Get(strDocumentId, null);
                JsonDocument.EnsureIdInMetadata(jsonDocument);
                var doc = jsonDocument.ToJson();
                doc[Constants.DocumentIdFieldName] = jsonDocument.Key;

                var docs = new List<ReplicatedDoc>
                           {
                               new ReplicatedDoc
                               {
                                   Document = doc,
                                   Etag = jsonDocument.Etag,
                                   Key = jsonDocument.Key,
                                   SerializedSizeOnDisk = jsonDocument.SerializedSizeOnDisk
                               }
                           };
                var scriptResult = ApplyConversionScript(sqlReplication, docs, stats);

                var sqlReplicationConnections = Database.ConfigurationRetriever.GetConfigurationDocument<SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin>>(Constants.SqlReplication.SqlReplicationConnectionsDocumentName);

                if (PrepareSqlReplicationConfig(sqlReplication, sqlReplication.Name, stats, sqlReplicationConnections.MergedDocument, false, false))
                {
                    if (performRolledbackTransaction)
                    {
                        using (var writer = new RelationalDatabaseWriter(Database, sqlReplication, stats))
                        {
                            resutls = writer.RolledBackExecute(scriptResult).ToArray();
                        }
                    }
                    else
                    {
                        var simulatedwriter = new RelationalDatabaseWriterSimulator(Database, sqlReplication, stats);
                        resutls = new List<RelationalDatabaseWriter.TableQuerySummary>
                        {
                            new RelationalDatabaseWriter.TableQuerySummary
                            {
                                Commands = simulatedwriter.SimulateExecuteCommandText(scriptResult)
                                    .Select(x => new RelationalDatabaseWriter.TableQuerySummary.CommandData
                                    {
                                        CommandText = x
                                    }).ToArray()
                            }
                        }.ToArray();


                    }
                }

                alert = stats.LastAlert;
            }
            catch (Exception e)
            {
                alert = new Alert
                {
                    AlertLevel = AlertLevel.Error,
                    CreatedAt = SystemTime.UtcNow,
                    Message = "Last SQL replication operation for " + sqlReplication.Name + " was failed",
                    Title = "SQL replication error",
                    Exception = e.ToString(),
                    UniqueKey = "Sql Replication Error: " + sqlReplication.Name
                };
            }
            return resutls;
        }

        public List<SqlReplicationConfig> GetConfiguredReplicationDestinations()
        {
            var sqlReplicationConfigs = replicationConfigs;
            if (sqlReplicationConfigs != null)
                return sqlReplicationConfigs;

            sqlReplicationConfigs = new List<SqlReplicationConfig>();
            Database.TransactionalStorage.Batch(accessor =>
            {
                const string Prefix = "Raven/SqlReplication/Configuration/";

                var configurationDocument = Database.ConfigurationRetriever.GetConfigurationDocument<SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin>>(Constants.SqlReplication.SqlReplicationConnectionsDocumentName);
                var sqlReplicationConnections = configurationDocument != null ? configurationDocument.MergedDocument : new SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin>(); // backward compatibility

                foreach (var sqlReplicationConfigDocument in accessor.Documents.GetDocumentsWithIdStartingWith(Prefix, 0, int.MaxValue, null))
                {
                    if (sqlReplicationConfigDocument == null)
                    {
                        continue;
                    }
                    var cfg = sqlReplicationConfigDocument.DataAsJson.JsonDeserialization<SqlReplicationConfig>();
                    var replicationStats = statistics.GetOrAdd(cfg.Name, name => new SqlReplicationStatistics(name));
                    if (!PrepareSqlReplicationConfig(cfg, sqlReplicationConfigDocument.Key, replicationStats, sqlReplicationConnections))
                        continue;
                    sqlReplicationConfigs.Add(cfg);
                }
            });
            replicationConfigs = sqlReplicationConfigs;
            return sqlReplicationConfigs;
        }

        private bool PrepareSqlReplicationConfig(SqlReplicationConfig cfg, string sqlReplicationConfigDocumentKey, SqlReplicationStatistics replicationStats, SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin> sqlReplicationConnections, bool writeToLog = true, bool validateSqlReplicationName = true)
        {
            if (validateSqlReplicationName && string.IsNullOrWhiteSpace(cfg.Name))
            {
                if (writeToLog)
                    Log.Warn("Could not find name for sql replication document {0}, ignoring", sqlReplicationConfigDocumentKey);
                replicationStats.LastAlert = new Alert
                {
                    AlertLevel = AlertLevel.Error,
                    CreatedAt = DateTime.UtcNow,
                    Title = "Could not start replication",
                    Message = string.Format("Could not find name for sql replication document {0}, ignoring", sqlReplicationConfigDocumentKey)
                };
                return false;
            }
            if (string.IsNullOrWhiteSpace(cfg.PredefinedConnectionStringSettingName) == false)
            {
                var matchingConnection = sqlReplicationConnections.PredefinedConnections.FirstOrDefault(x => string.Compare(x.Name, cfg.PredefinedConnectionStringSettingName, StringComparison.InvariantCultureIgnoreCase) == 0);
                if (matchingConnection != null)
                {
                    cfg.ConnectionString = matchingConnection.ConnectionString;
                    cfg.FactoryName = matchingConnection.FactoryName;
                }
                else
                {
                    if (writeToLog)
                        Log.Warn("Could not find predefined connection string named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
                            cfg.PredefinedConnectionStringSettingName,
                            sqlReplicationConfigDocumentKey);
                    replicationStats.LastAlert = new Alert
                    {
                        AlertLevel = AlertLevel.Error,
                        CreatedAt = DateTime.UtcNow,
                        Title = "Could not start replication",
                        Message = string.Format("Could not find predefined connection string named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
                            cfg.PredefinedConnectionStringSettingName,
                            sqlReplicationConfigDocumentKey)
                    };
                    return false;
                }
            }
            else if (string.IsNullOrWhiteSpace(cfg.ConnectionStringName) == false)
            {
                var connectionString = System.Configuration.ConfigurationManager.ConnectionStrings[cfg.ConnectionStringName];
                if (connectionString == null)
                {
                    if (writeToLog)
                        Log.Warn("Could not find connection string named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
                            cfg.ConnectionStringName, sqlReplicationConfigDocumentKey);

                    replicationStats.LastAlert = new Alert
                    {
                        AlertLevel = AlertLevel.Error,
                        CreatedAt = DateTime.UtcNow,
                        Title = "Could not start replication",
                        Message = string.Format("Could not find connection string named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
                            cfg.ConnectionStringName,
                            sqlReplicationConfigDocumentKey)
                    };
                    return false;
                }
                cfg.ConnectionString = connectionString.ConnectionString;
            }
            else if (string.IsNullOrWhiteSpace(cfg.ConnectionStringSettingName) == false)
            {
                var setting = Database.Configuration.Settings[cfg.ConnectionStringSettingName];
                if (string.IsNullOrWhiteSpace(setting))
                {
                    if (writeToLog)
                        Log.Warn("Could not find setting named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
            cfg.ConnectionStringSettingName,
            sqlReplicationConfigDocumentKey);
                    replicationStats.LastAlert = new Alert
                    {
                        AlertLevel = AlertLevel.Error,
                        CreatedAt = DateTime.UtcNow,
                        Title = "Could not start replication",
                        Message = string.Format("Could not find setting named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
                            cfg.ConnectionStringSettingName,
                            sqlReplicationConfigDocumentKey)
                    };
                    return false;
                }
            }
            return true;
        }

        public void Dispose()
        {
            foreach (var prefetchingBehavior in prefetchingBehaviors)
            {
                prefetchingBehavior.Dispose();
            }
        }

        private class SqlConfigGroup
        {
            public Etag LastReplicatedEtag { get; set; }

            public List<SqlReplicationConfigWithLastReplicatedEtag> ConfigsToWorkOn { get; set; }

            public PrefetchingBehavior PrefetchingBehavior { get; set; }
        }
    }
}
