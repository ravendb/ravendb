using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Server.Documents;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.ServerWide.Maintenance
{
    public class ClusterMaintenanceWorker : IDisposable
    {
        private readonly TcpConnectionOptions _tcp;
        private readonly ServerStore _server;
        private CancellationToken _token;
        private readonly CancellationTokenSource _cts;
        private Task _collectingTask;
        private readonly Logger _logger;

        public readonly long CurrentTerm;

        public readonly TimeSpan WorkerSamplePeriod;

        public ClusterMaintenanceWorker(TcpConnectionOptions tcp, CancellationToken externalToken, ServerStore serverStore, long term)
        {
            _tcp = tcp;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
            _token = _cts.Token;
            _server = serverStore;
            WorkerSamplePeriod = _server.Configuration.Cluster.WorkerSamplePeriod.AsTimeSpan;
            _logger = LoggingSource.Instance.GetLogger<ClusterMaintenanceWorker>($"Logger on {serverStore.NodeTag}");
            CurrentTerm = term;
        }

        public void Start()
        {
            _collectingTask = CollectDatabasesStatusReport();
        }

        public async Task CollectDatabasesStatusReport()
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
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"Exception occurred while collecting info from {_server.NodeTag}", e);
                    }
                }
                finally
                {
                    await TimeoutManager.WaitFor(WorkerSamplePeriod, _token);
                }
            }
        }

        private IEnumerable<(string name, DatabaseStatusReport report)> CollectDatabaseInformation(TransactionOperationContext ctx)
        {
            foreach (var dbName in _server.Cluster.GetDatabaseNames(ctx))
            {
                if (_token.IsCancellationRequested)
                    yield break;

                if (_server.DatabasesLandlord.DatabasesCache.TryGetValue(dbName, out var dbTask) == false)
                {
                    yield return (dbName, new DatabaseStatusReport
                    {
                        Name = dbName,
                        NodeName = _server.NodeTag,
                        Status = DatabaseStatus.Unloaded
                    });
                    continue;
                }

                var report = new DatabaseStatusReport
                {
                    Name = dbName,
                    NodeName = _server.NodeTag,
                };

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
                        report.LastDocumentChangeVector = documentsStorage.GetDatabaseChangeVector(context);

                        if (indexStorage != null)
                        {
                            foreach (var index in indexStorage.GetIndexes())
                            {
                                var stats = index.GetIndexStats(context);

                                report.LastIndexStats.Add(index.Name, new DatabaseStatusReport.ObservedIndexStatus
                                {
                                    LastIndexedEtag = stats.lastProcessedEtag,
                                    IsSideBySide = false, // TODO: fix this so it get whatever this has side by side or not
                                    IsStale = stats.isStale
                                });
                            }
                        }
                    }
                }
                catch(Exception e)
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
                if (_collectingTask.Wait(TimeSpan.FromSeconds(30)) == false)
                {
                    _collectingTask.IgnoreUnobservedExceptions();

                    throw new ObjectDisposedException($"Collecting report task on {_server.NodeTag} still running and can't be closed");
                }
            }
            finally
            {
                _cts.Dispose();
            }
        }

    }
}

