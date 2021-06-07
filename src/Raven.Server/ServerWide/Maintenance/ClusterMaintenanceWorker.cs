using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Debugging;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Index = Raven.Server.Documents.Indexes.Index;
using Sparrow.LowMemory;
using Sparrow.Server.Json.Sync;
using Sparrow.Server.LowMemory;

namespace Raven.Server.ServerWide.Maintenance
{
    public class ClusterMaintenanceWorker : IDisposable
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

        public ClusterMaintenanceWorker(TcpConnectionOptions tcp, CancellationToken externalToken, ServerStore serverStore, string leader, long term)
        {
            _tcp = tcp;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
            _token = _cts.Token;
            _server = serverStore;
            _logger = LoggingSource.Instance.GetLogger<ClusterMaintenanceWorker>(serverStore.NodeTag);
            _name = $"Heartbeats worker connection to leader {leader} in term {term}";
            _temporaryDirtyMemoryAllowedPercentage = _server.Server.ServerStore.Configuration.Memory.TemporaryDirtyMemoryAllowedPercentage;

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
            , null, _name);
        }

        public void CollectDatabasesStatusReport()
        {
            var lastNodeReport = new Dictionary<string, DatabaseStatusReport>();
            var report = new MaintenanceReport();
            while (_token.IsCancellationRequested == false)
            {
                try
                {
                    using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
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

        private void HeartbeatVersion41200(TransactionOperationContext ctx, Dictionary<string, DatabaseStatusReport> nodeReport)
        {
            using (var writer = new BlittableJsonTextWriter(ctx, _tcp.Stream))
            {
                ctx.Write(writer, DynamicJsonValue.Convert(nodeReport));
            }
        }

        private void HeartbeatVersion42000(TransactionOperationContext ctx, MaintenanceReport report)
        {
            report.ServerReport = new ServerReport
            {
                OutOfCpuCredits = _server.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised(),
                EarlyOutOfMemory = LowMemoryNotification.Instance.IsEarlyOutOfMemory,
                HighDirtyMemory = LowMemoryNotification.Instance.DirtyMemoryState.IsHighDirty
            };

            using (var writer = new BlittableJsonTextWriter(ctx, _tcp.Stream))
            {
                ctx.Write(writer, report.ToJson());
            }
        }

        private Dictionary<string, DatabaseStatusReport> CollectDatabaseInformation(TransactionOperationContext ctx, Dictionary<string, DatabaseStatusReport> prevReport)
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

                    foreach (var tuple in rawRecord.Topologies)
                    {
                        var dbName = tuple.Name;
                        var topology = tuple.Topology;

                        var report = new DatabaseStatusReport
                        {
                            Name = dbName, 
                            NodeName = _server.NodeTag
                        };
                        
                        if (topology == null)
                        {
                            continue;
                        }

                        if (topology.RelevantFor(_server.NodeTag) == false)
                        {
                            continue;
                        }

                        if (_server.DatabasesLandlord.DatabasesCache.TryGetValue(dbName, out var dbTask) == false)
                        {
                            report.Status = DatabaseStatus.Unloaded;
                            result[dbName] = report;
                            continue;
                        }

                        if (dbTask.IsFaulted)
                        {
                            var extractSingleInnerException = dbTask.Exception.ExtractSingleInnerException();
                            if (Equals(extractSingleInnerException.Data[DatabasesLandlord.DoNotRemove], true))
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

                            if (_server.IdleDatabases.ContainsKey(dbName))
                                report.UpTime = TimeSpan.MinValue;

                            result[dbName] = report;
                            continue;
                        }

                        var dbInstance = dbTask.Result;
                        var currentHash = dbInstance.GetEnvironmentsHash();
                        report.EnvironmentsHash = currentHash;

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
                            report.UpTime = now - dbInstance.StartTime;

                            FillReplicationInfo(dbInstance, report);

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
                                FillClusterTransactionInfo(report, dbInstance);

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

        private static void FillClusterTransactionInfo(DatabaseStatusReport report, DocumentDatabase dbInstance)
        {
            report.LastTransactionId = dbInstance.LastTransactionId;
            report.LastCompletedClusterTransaction = dbInstance.LastCompletedClusterTransaction;
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
                    report.LastEtag = DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
                    report.LastTombstoneEtag = DocumentsStorage.ReadLastTombstoneEtag(tx.InnerTransaction);
                    report.NumberOfConflicts = documentsStorage.ConflictsStorage.ConflictsCount;
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
                    report.LastSentEtag.Add(node, outgoing._lastSentDocumentEtag);
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

