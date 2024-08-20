using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance.Sharding;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Sparrow.Logging;
using Index = Raven.Server.Documents.Indexes.Index;
using Sparrow.LowMemory;
using Sparrow.Server.Utils;

namespace Raven.Server.ServerWide.Maintenance
{
    public sealed class ClusterMaintenanceWorker : IDisposable
    {
        private readonly TcpConnectionOptions _tcp;
        private readonly ServerStore _server;
        private CancellationToken _token;
        private readonly CancellationTokenSource _cts;
        private readonly Logger _logger;

        private readonly string _name;
        public readonly long CurrentTerm;

        public readonly TimeSpan WorkerSamplePeriod;
        private PoolOfThreads.LongRunningWork _collectingTask;
        public readonly TcpConnectionHeaderMessage.SupportedFeatures SupportedFeatures;
        private readonly float _temporaryDirtyMemoryAllowedPercentage;
        private readonly long _term;
        private readonly string _leader;

        public ClusterMaintenanceWorker(TcpConnectionOptions tcp, CancellationToken externalToken, ServerStore serverStore, string leader, long term)
        {
            _tcp = tcp;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
            _token = _cts.Token;
            _server = serverStore;
            _logger = LoggingSource.Instance.GetLogger<ClusterMaintenanceWorker>(serverStore.NodeTag);
            _name = $"Heartbeats worker connection to leader {leader} in term {term}";
            _temporaryDirtyMemoryAllowedPercentage = _server.Server.ServerStore.Configuration.Memory.TemporaryDirtyMemoryAllowedPercentage;
            _leader = leader;
            _term = term;

            WorkerSamplePeriod = _server.Configuration.Cluster.WorkerSamplePeriod.AsTimeSpan;
            CurrentTerm = term;
            SupportedFeatures = TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Heartbeats, _tcp.ProtocolVersion);
        }

        public void Start()
        {
            _collectingTask = PoolOfThreads.GlobalRavenThreadPool.LongRunning(_ =>
            {
                try
                {
                    CollectDatabasesStatusReport();
                }
                catch (ObjectDisposedException)
                {
                    // expected
                }
                catch (OperationCanceledException)
                {
                    // expected
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"Exception occurred while collecting info from {_server.NodeTag}. Task is closed.", e);
                    }
                    // we don't want to crash the process so we don't propagate this exception.
                }
            }
                , null, ThreadNames.ForHeartbeatsWorker(_name, _leader, _term));
        }

        public void CollectDatabasesStatusReport()
        {
            var lastNodeReport = new Dictionary<string, DatabaseStatusReport>();
            var report = new MaintenanceReport();
            while (_token.IsCancellationRequested == false)
            {
                try
                {
                    using (_server.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                    {
                        Dictionary<string, DatabaseStatusReport> nodeReport;
                        using (ctx.OpenReadTransaction())
                        {
                            nodeReport = CollectDatabaseInformation(ctx, lastNodeReport);
                        }

                        if (SupportedFeatures.Heartbeats.IncludeServerInfo == false)
                        {
                            HeartbeatVersion41200(ctx, nodeReport);
                        }
                        else
                        {
                            report.DatabasesReport = nodeReport;
                            HeartbeatVersion42000(ctx, report);
                        }

                        lastNodeReport = nodeReport;
                    }
                }
                catch (Exception e)
                {
                    if (_tcp.TcpClient?.Connected != true)
                    {
                        if (_logger.IsInfoEnabled)
                        {
                            _logger.Info("The tcp connection was closed, so we exit the maintenance work.");
                        }
                        return;
                    }
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"Exception occurred while collecting info from {_server.NodeTag}", e);
                    }
                }
                finally
                {
                    _token.WaitHandle.WaitOne(WorkerSamplePeriod);
                }
            }
        }

        private void HeartbeatVersion41200(JsonOperationContext ctx, Dictionary<string, DatabaseStatusReport> nodeReport)
        {
            using (var writer = new BlittableJsonTextWriter(ctx, _tcp.Stream))
            {
                ctx.Write(writer, DynamicJsonValue.Convert(nodeReport));
            }
        }

        private void HeartbeatVersion42000(ClusterOperationContext ctx, MaintenanceReport report)
        {
            long index;
            using (ctx.OpenReadTransaction())
            {
                _server.Engine.GetLastCommitIndex(ctx, out index, out _);
            }

            report.ServerReport = new ServerReport
            {
                OutOfCpuCredits = _server.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised(),
                EarlyOutOfMemory = LowMemoryNotification.Instance.IsEarlyOutOfMemory,
                HighDirtyMemory = LowMemoryNotification.Instance.DirtyMemoryState.IsHighDirty,
                LastCommittedIndex = index
            };

            using (var writer = new BlittableJsonTextWriter(ctx, _tcp.Stream))
            {
                ctx.Write(writer, report.ToJson());
            }
        }

        private Dictionary<string, DatabaseStatusReport> CollectDatabaseInformation(ClusterOperationContext ctx, Dictionary<string, DatabaseStatusReport> prevReport)
        {
            var result = new Dictionary<string, DatabaseStatusReport>();
            foreach (var databaseName in _server.Cluster.GetDatabaseNames(ctx))
            {
                if (_token.IsCancellationRequested)
                    return result;

                using (var rawRecord = _server.Cluster.ReadRawDatabaseRecord(ctx, databaseName))
                {
                    if (rawRecord == null)
                    {
                        continue; // Database does not exists in this server
                    }

                    var sharding = rawRecord.Sharding;
                    var currentMigration = sharding?.BucketMigrations.SingleOrDefault(pair => pair.Value.IsActive).Value;
                    
                    //create mock database report for the sharded db for the cluster observer topology update
                    if (rawRecord.IsSharded && sharding.Orchestrator.Topology.RelevantFor(_server.NodeTag))
                    {
                        var report = new DatabaseStatusReport()
                        {
                            Status = DatabaseStatus.Loaded,
                            Name = databaseName,
                            NodeName = _server.NodeTag,
                            UpTime = _server.Server.Statistics.UpTime
                        };
                        result[databaseName] = report;
                    }

                    foreach (var tuple in rawRecord.Topologies)
                    {
                        var dbName = tuple.Name;
                        var topology = tuple.Topology;

                        var report = new DatabaseStatusReport { Name = dbName, NodeName = _server.NodeTag };

                        if (topology == null)
                        {
                            continue;
                        }

                        if (topology.RelevantFor(_server.NodeTag) == false)
                        {
                            continue;
                        }

                        if (_server.DatabasesLandlord.DatabasesCache.TryGetValue(dbName, out var dbTask, out var details) == false)
                        {
                            report.Status = DatabaseStatus.Unloaded;
                            result[dbName] = report;
                            continue;
                        }

                        report.UpTime = SystemTime.UtcNow - details.InCacheSince;

                        if (dbTask.IsFaulted)
                        {
                            if (DatabasesLandlord.IsLockedDatabase(dbTask.Exception))
                            {
                                report.Status = DatabaseStatus.Unloaded;
                                result[dbName] = report;
                                continue;
                            }
                        }

                        if (dbTask.IsCanceled || dbTask.IsFaulted)
                        {
                            report.Status = DatabaseStatus.Faulted;
                            report.Error = dbTask.Exception.ToString();
                            result[dbName] = report;
                            continue;
                        }

                        if (dbTask.IsCompleted == false)
                        {
                            report.Status = DatabaseStatus.Loading;
                            result[dbName] = report;
                            continue;
                        }

                        var dbInstance = dbTask.Result;
                        var currentHash = dbInstance.GetEnvironmentsHash();
                        report.EnvironmentsHash = currentHash;
                        FillClusterTransactionInfo(ctx, dbInstance, report);

                        var documentsStorage = dbInstance.DocumentsStorage;
                        var indexStorage = dbInstance.IndexStore;

                        if (dbInstance.DatabaseShutdown.IsCancellationRequested)
                        {
                            report.Status = DatabaseStatus.Shutdown;
                            result[dbName] = report;
                            continue;
                        }

                        report.Status = DatabaseStatus.Loaded;
                        try
                        {
                            var now = dbInstance.Time.GetUtcNow();
                            FillReplicationInfo(dbInstance, report);

                            if (rawRecord.IsSharded)
                            {
                                if (currentMigration != null)
                                {
                                    if (report.ReportPerBucket.TryGetValue(currentMigration.Bucket, out var bucketReport) == false)
                                    {
                                        bucketReport = new BucketReport();
                                    }

                                    var shardedInstance = dbInstance as ShardedDocumentDatabase;
                                    using (shardedInstance.ShardedDocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                                    using (context.OpenReadTransaction())
                                    {
                                        bucketReport.LastChangeVector = shardedInstance.ShardedDocumentsStorage.GetMergedChangeVectorInBucket(context, currentMigration.Bucket);
                                       
                                        var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(context, currentMigration.Bucket);
                                        if (stats != null)
                                        {
                                            bucketReport.NumberOfDocuments = stats.NumberOfDocuments;
                                            bucketReport.Size = stats.Size;
                                            bucketReport.LastAccess = stats.LastModified;
                                        }
                                    }

                                    report.ReportPerBucket[currentMigration.Bucket] = bucketReport;
                                }
                            }

                            prevReport.TryGetValue(dbName, out var prevDatabaseReport);
                            if (SupportedFeatures.Heartbeats.SendChangesOnly &&
                                prevDatabaseReport != null && prevDatabaseReport.EnvironmentsHash == currentHash)
                            {
                                report.Status = DatabaseStatus.NoChange;
                                result[dbName] = report;
                                continue;
                            }

                            using (var context = QueryOperationContext.Allocate(dbInstance, needsServerContext: true))
                            {
                                FillDocumentsInfo(prevDatabaseReport, dbInstance, report, context.Documents, documentsStorage);

                                if (indexStorage != null)
                                {
                                    foreach (var index in indexStorage.GetIndexes())
                                    {
                                        DatabaseStatusReport.ObservedIndexStatus stat = null;
                                        if (prevDatabaseReport?.LastIndexStats.TryGetValue(index.Name, out stat) == true &&
                                            stat?.LastTransactionId == index.LastTransactionId)
                                        {
                                            report.LastIndexStats[index.Name] = stat;
                                            continue;
                                        }

                                        using (context.OpenReadTransaction())
                                        {
                                            FillIndexInfo(index, context, now, report);
                                        }
                                    }
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            report.EnvironmentsHash = 0; // on error we should do the complete report collaction path
                            report.Error = e.ToString();
                        }

                        result[dbName] = report;
                    }
                }
            }

            return result;
        }

        private void FillClusterTransactionInfo(ClusterOperationContext ctx, DocumentDatabase dbInstance, DatabaseStatusReport report)
        {
            report.LastCompareExchangeIndex = dbInstance.CompareExchangeStorage.GetLastCompareExchangeIndex(ctx);
            report.LastCompletedClusterTransaction = dbInstance.LastCompletedClusterTransaction;
            report.LastClusterWideTransactionRaftIndex = dbInstance.ClusterWideTransactionIndexWaiter.LastIndex;
        }

        private static void FillIndexInfo(Index index, QueryOperationContext context, DateTime now, DatabaseStatusReport report)
        {
            var stats = index.GetIndexingState(context);
            var lastQueried = GetLastQueryInfo(index, now);

            //We might have old version of this index with the same name
            report.LastIndexStats[index.Name] = new DatabaseStatusReport.ObservedIndexStatus
            {
                LastIndexedEtag = stats.LastProcessedEtag,
                LastIndexedCompareExchangeReferenceEtag = stats.LastProcessedCompareExchangeReferenceEtag,
                LastIndexedCompareExchangeReferenceTombstoneEtag = stats.LastProcessedCompareExchangeReferenceTombstoneEtag,
                LastQueried = lastQueried,
                IsSideBySide = index.Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix, StringComparison.OrdinalIgnoreCase),
                IsStale = stats.IsStale,
                State = index.State,
                LastTransactionId = index.LastTransactionId
            };
        }

        private static TimeSpan? GetLastQueryInfo(Index index, DateTime now)
        {
            TimeSpan? lastQueried = null;
            var lastQueryingTime = index.GetLastQueryingTime();
            if (lastQueryingTime.HasValue)
                lastQueried = now - lastQueryingTime;
            return lastQueried;
        }

        private static void FillDocumentsInfo(DatabaseStatusReport prevDatabaseReport, DocumentDatabase dbInstance, DatabaseStatusReport report,
            DocumentsOperationContext context, DocumentsStorage documentsStorage)
        {
            report.LastTransactionId = dbInstance.LastTransactionId;
            if (prevDatabaseReport?.LastTransactionId != null && prevDatabaseReport.LastTransactionId == dbInstance.LastTransactionId)
            {
                report.LastEtag = prevDatabaseReport.LastEtag;
                report.LastTombstoneEtag = prevDatabaseReport.LastTombstoneEtag;
                report.NumberOfConflicts = prevDatabaseReport.NumberOfConflicts;
                report.NumberOfDocuments = prevDatabaseReport.NumberOfDocuments;
                report.DatabaseChangeVector = prevDatabaseReport.DatabaseChangeVector;
            }
            else
            {
                using (var tx = context.OpenReadTransaction())
                {
                    report.LastEtag = documentsStorage.ReadLastEtag(tx.InnerTransaction);
                    report.LastTombstoneEtag = DocumentsStorage.ReadLastTombstoneEtag(tx.InnerTransaction);
                    report.NumberOfConflicts = documentsStorage.ConflictsStorage.GetNumberOfConflicts(tx.InnerTransaction);
                    report.NumberOfDocuments = documentsStorage.GetNumberOfDocuments(context);
                    report.DatabaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);
                }
            }
        }

        private static void FillReplicationInfo(DocumentDatabase dbInstance, DatabaseStatusReport report)
        {
            foreach (var outgoing in dbInstance.ReplicationLoader.OutgoingHandlers)
            {
                var node = outgoing.GetNode();
                if (node != null)
                {
                    report.LastSentEtag.Add(node, outgoing.LastSentDocumentEtag);
                }
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
            _tcp.Dispose();

            try
            {
                if (_collectingTask == null)
                    return;

                if (_collectingTask.ManagedThreadId == Thread.CurrentThread.ManagedThreadId)
                    return;

                if (_collectingTask.Join((int)TimeSpan.FromSeconds(30).TotalMilliseconds) == false)
                {
                    throw new ObjectDisposedException($"{_name} still running and can't be closed");
                }
            }
            finally
            {
                _cts.Dispose();
            }
        }
    }
}

