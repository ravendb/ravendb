using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Rachis
{
    public class ClusterMaintenanceSlave : IDisposable
    {
        private readonly TcpConnectionOptions _tcp;
        private readonly ServerStore _server;
        private CancellationToken _token;
        private readonly CancellationTokenSource _cts;
        private Task _collectingTask;
        private Dictionary<string,DatabaseStatusReport> _cachedReport = new Dictionary<string, DatabaseStatusReport>();

        public ClusterMaintenanceSlave(TcpConnectionOptions tcp, CancellationToken externalToken, ServerStore serverStore)
        {
            _tcp = tcp;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
            _token = _cts.Token;
            _server = serverStore;
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
                            ctx.Write(writer, DynamicJsonValue.Convert(dbs.ToDictionary(k => k.Item1,v=>v.Item2)));
                        }
                    }
                    
                    await Task.Delay(TimeSpan.FromMilliseconds(500), _token);
                }
                catch (Exception)
                {
                    // TODO: log and abort
                }
            }
        }

        private IEnumerable<(string,DatabaseStatusReport)> CollectDatabaseInformation(TransactionOperationContext ctx)
        {
            foreach (var dbName in _server.Cluster.GetDatabaseNames(ctx))
            {
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

                using (documentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    report.LastDocumentChangeVector = documentsStorage.GetDatabaseChangeVector(context);
                }
                foreach (var index in indexStorage.GetIndexes())
                {
                    report.LastIndexedDocumentEtag.Add(index.Name, index.Etag);
                }

                yield return (dbName, report);
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
            _tcp.Dispose();
            if (_collectingTask.Wait(TimeSpan.FromSeconds(5)) == false)
            {
                throw new ObjectDisposedException($"Collecting report task on {_server.NodeTag} still running and can't be closed");
            }
        }

        //TODO: consider creating finalizer to absolutely make sure we dispose the socket
    }
}
