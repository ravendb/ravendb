using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.ServerWide.Maintance
{
    public class ClusterMaintenanceSlave : IDisposable
    {
        private readonly TcpConnectionOptions _tcp;
        private readonly ServerStore _server;
        private CancellationToken _token;
        private readonly CancellationTokenSource _cts;
        private Task _collectingTask;
        private readonly Logger _logger;

        public readonly long CurrentTerm;

        public readonly long NodeSamplePeriod;

        public ClusterMaintenanceSlave(TcpConnectionOptions tcp, CancellationToken externalToken, ServerStore serverStore,long term)
        {
            _tcp = tcp;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
            _token = _cts.Token;
            _server = serverStore;
            NodeSamplePeriod = (long)_server.Configuration.ClusterMaintaince.NodeSamplePeriod.AsTimeSpan.TotalMilliseconds;
            _logger = LoggingSource.Instance.GetLogger<ClusterMaintenanceSlave>($"Logger on {serverStore.NodeTag}");
            CurrentTerm = term;
        }

        public void Start()
        {
            _collectingTask = CollectReport();
        }

        public async Task CollectReport()
        {
            while (_token.IsCancellationRequested == false)
            {
                try
                {
                    using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var dbs = CollectDatabaseInformation(ctx);
                        using (var writer = new BlittableJsonTextWriter(ctx, _tcp.Stream))
                        {
                            ctx.Write(writer, DynamicJsonValue.Convert(dbs.ToDictionary(k => k.Item1, v => v.Item2)));
                        }
                    }
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"Exception occured while collecting info from {_server.NodeTag}", e);
                    }
                    return;
                }
                finally
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(NodeSamplePeriod), _token);
                }
            }
        }

        private IEnumerable<(string,DatabaseStatusReport)> CollectDatabaseInformation(TransactionOperationContext ctx)
        {
            foreach (var dbName in _server.Cluster.GetDatabaseNames(ctx))
            {
                if (_token.IsCancellationRequested)
                    yield break;

                DatabaseStatusReport report;
                if (_server.DatabasesLandlord.DatabasesCache.TryGetValue(dbName, out var dbTask) == false)
                {
                    report = new DatabaseStatusReport
                    {
                        Name = dbName,
                        NodeName = _server.NodeTag,
                        Status = DatabaseStatus.Unloaded
                    };
                    yield return (dbName, report);
                       
                    continue;
                }

                report = new DatabaseStatusReport
                {
                    Name = dbName,
                    NodeName = _server.NodeTag,
                    Status = DatabaseStatus.Loading
                };

                if (dbTask.IsCanceled || dbTask.IsFaulted)
                {
                    report.Status = DatabaseStatus.Faulted;
                    report.FailureToLoad = dbTask.Exception.ToString();
                    yield return (dbName, report);
                    continue;
                }

                if (dbTask.IsCompleted == false)
                {
                    yield return (dbName, report);
                    continue;
                }

                report.Status = DatabaseStatus.Loaded;

                var dbInstance = dbTask.Result;
                var documentsStorage = dbInstance.DocumentsStorage;
                var indexStorage = dbInstance.IndexStore;

                if (_token.IsCancellationRequested)
                    yield break;

                using (documentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    report.LastEtag = DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
                    report.LastTombstoneEtag = DocumentsStorage.ReadLastTombstoneEtag(tx.InnerTransaction);
                    report.NumberOfConflicts = documentsStorage.ConflictsStorage.ConflictsCount;
                    report.LastDocumentChangeVector = documentsStorage.GetDatabaseChangeVector(context);
                }
                if (indexStorage != null)
                {
                    foreach (var index in indexStorage.GetIndexes())
                    {
                        report.LastIndexedDocumentEtag.Add(index.Name, report.LastEtag - index.Etag);
                    }
                }
                yield return (dbName, report);
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
            _tcp.Dispose();
            if (_collectingTask.Wait(TimeSpan.FromSeconds(30)) == false)
            {
                throw new ObjectDisposedException($"Collecting report task on {_server.NodeTag} still running and can't be closed");
            }
            _cts.Dispose();
        }

        //TODO: consider creating finalizer to absolutely make sure we dispose the socket
    }
}
