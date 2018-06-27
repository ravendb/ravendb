using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client;
using Raven.Client.Extensions;
using Raven.Server.Documents;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

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

        public ClusterMaintenanceWorker(TcpConnectionOptions tcp, CancellationToken externalToken, ServerStore serverStore, string leader, long term)
        {
            _tcp = tcp;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
            _token = _cts.Token;
            _server = serverStore;
            _logger = LoggingSource.Instance.GetLogger<ClusterMaintenanceWorker>(serverStore.NodeTag);
            _name = $"Maintenance worker connection to leader {leader} in term {term}";

            WorkerSamplePeriod = _server.Configuration.Cluster.WorkerSamplePeriod.AsTimeSpan;
            CurrentTerm = term;
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
            while (_token.IsCancellationRequested == false)
            {
                try
                {
                    using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var dbs = CollectDatabaseInformation(ctx);
                        var djv = new DynamicJsonValue();
                        foreach (var tuple in dbs)
                        {
                            djv[tuple.name] = tuple.report;
                        }
                        using (var writer = new BlittableJsonTextWriter(ctx, _tcp.Stream))
                        {
                            ctx.Write(writer, djv);
                        }
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

        private IEnumerable<(string name, DatabaseStatusReport report)> CollectDatabaseInformation(TransactionOperationContext ctx)
        {
            foreach (var dbName in _server.Cluster.GetDatabaseNames(ctx))
            {
                if (_token.IsCancellationRequested)
                    yield break;

                var report = new DatabaseStatusReport
                {
                    Name = dbName,
                    NodeName = _server.NodeTag
                };

                if (_server.DatabasesLandlord.DatabasesCache.TryGetValue(dbName, out var dbTask) == false)
                {

                    var recorod = _server.Cluster.ReadDatabase(ctx, dbName);
                    if (recorod == null || recorod.Topology.RelevantFor(_server.NodeTag) == false)
                    {
                        continue; // Database does not exists in this server
                    }
                    report.Status = DatabaseStatus.Unloaded;
                    yield return (dbName, report);
                    continue;
                }

                if (dbTask.IsFaulted)
                {
                    var extractSingleInnerException = dbTask.Exception.ExtractSingleInnerException();
                    if (Equals(extractSingleInnerException.Data[DatabasesLandlord.DoNotRemove], true))
                    {
                        report.Status = DatabaseStatus.Unloaded;
                        yield return (dbName, report);
                        continue;
                    }
                }


                if (dbTask.IsCanceled || dbTask.IsFaulted)
                {
                    report.Status = DatabaseStatus.Faulted;
                    report.Error = dbTask.Exception.ToString();
                    yield return (dbName, report);
                    continue;
                }

                if (dbTask.IsCompleted == false)
                {
                    report.Status = DatabaseStatus.Loading;
                    yield return (dbName, report);
                    continue;
                }

                var dbInstance = dbTask.Result;
                var documentsStorage = dbInstance.DocumentsStorage;
                var indexStorage = dbInstance.IndexStore;

                if (dbInstance.DatabaseShutdown.IsCancellationRequested)
                {
                    report.Status = DatabaseStatus.Shutdown;
                    yield return (dbName, report);
                    continue;
                }

                report.Status = DatabaseStatus.Loaded;
                try
                {
                    using (documentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        report.LastEtag = DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
                        report.LastTombstoneEtag = DocumentsStorage.ReadLastTombstoneEtag(tx.InnerTransaction);
                        report.NumberOfConflicts = documentsStorage.ConflictsStorage.ConflictsCount;
                        report.NumberOfDocuments = documentsStorage.GetNumberOfDocuments(context);
                        report.DatabaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);
                        foreach (var outgoing in dbInstance.ReplicationLoader.OutgoingHandlers)
                        {
                            var node = outgoing.GetNode();
                            if (node != null)
                            {
                                report.LastSentEtag.Add(node, outgoing._lastSentDocumentEtag);
                            }
                        }

                        if (indexStorage != null)
                        {
                            foreach (var index in indexStorage.GetIndexes())
                            {
                                var stats = index.GetIndexStats(context);
                                //We might have old version of this index with the same name
                                report.LastIndexStats[index.Name] = new DatabaseStatusReport.ObservedIndexStatus
                                {
                                    LastIndexedEtag = stats.LastProcessedEtag,
                                    IsSideBySide = index.Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix, StringComparison.OrdinalIgnoreCase),
                                    IsStale = stats.IsStale,
                                    State = index.State
                                };
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    report.Error = e.ToString();
                }

                yield return (dbName, report);
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

