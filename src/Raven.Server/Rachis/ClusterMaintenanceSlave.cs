using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Rachis
{
    public class ClusterMaintenanceSlave : IDisposable
    {
        private readonly TcpConnectionOptions _tcp;

        public ClusterMaintenanceSlave(TcpConnectionOptions tcp, CancellationToken token, ServerStore serverStore)
        {
            _tcp = tcp;

            Task.Factory.StartNew(() =>
            {
                var statusReport = new ClusterNodeStatusReport
                {
                    ClusterTag = serverStore.NodeTag,
                    LastAttachmentChangeVectorPerDatabase = new Dictionary<string, ChangeVectorEntry[]>(),
                    LastIndexedDocumentEtagPerDatabase = new Dictionary<string, long>(),
                    LastDocumentChangeVectorPerDatabase = new Dictionary<string, ChangeVectorEntry[]>()
                };

                while (token.IsCancellationRequested == false)
                {                    
                    statusReport.Clear();
                    using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    {                        
                        foreach (var databaseName in serverStore.Cluster.GetDatabaseNames(ctx))
                        {
                            //precaution, 
                            if (TryGetDatabaseMaintenanceInfoFromCache( 
                                    databaseName,
                                    out ChangeVectorEntry[] lastDocumentChangeVector,
                                    out ChangeVectorEntry[] lastAttachmentChangeVector,
                                    out long lastIndexedDocumentEtag) == false)
                            {
                                //TODO: add logging, this shouldn't happen. 
                                continue;
                            }

                            statusReport.LastDocumentChangeVectorPerDatabase.Add(databaseName,lastDocumentChangeVector);
                            statusReport.LastAttachmentChangeVectorPerDatabase.Add(databaseName, lastAttachmentChangeVector);
                            statusReport.LastIndexedDocumentEtagPerDatabase.Add(databaseName, lastIndexedDocumentEtag);
                        }
                    }

                    Thread.Sleep(500);
                }
            }, token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        //TODO: implement such cache, so even if the database is unloaded, it's stuff will be saved here
        //the idea is that we do not want to wake up database in order to get it's data
        //(eventually a database will catch up with replication, and if it has nothing to do it will go to "sleep")
        private bool TryGetDatabaseMaintenanceInfoFromCache(
            string databaseName, 
            out ChangeVectorEntry[] lastDocumentChangeVector, 
            out ChangeVectorEntry[] lastAttachmentChangeVector, 
            out long lastIndexedDocumentEtag)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {            
            _tcp.Dispose();
        }

        //TODO: consider creating finalizer to absolutely make sure we dispose the socket
    }
}
