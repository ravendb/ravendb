using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Replication.Messages;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationClusterTopologyExplorer : IDisposable
    {
        private readonly DocumentDatabase _database;
        private readonly Dictionary<string, List<string>> _alreadyKnownDestinations;
        private readonly TimeSpan _timeout;
        private readonly List<ReplicationTopologyDestinationExplorer> _discoverers;
        private readonly List<IDisposable> _disposables = new List<IDisposable>();
        public int DiscovererCount => _discoverers.Count;

        public ReplicationClusterTopologyExplorer(
            DocumentDatabase database,
            Dictionary<string, List<string>> alreadyKnownDestinations,
            TimeSpan timeout, 
            List<ReplicationDestination> replicationDestinations)
        {
            _database = database;
            _alreadyKnownDestinations = alreadyKnownDestinations;
            _timeout = timeout;
            _discoverers = new List<ReplicationTopologyDestinationExplorer>();
            var dbId = _database.DbId.ToString();

            List<string> destinationIds;
            if (!alreadyKnownDestinations.TryGetValue(dbId, out destinationIds))
            {
                destinationIds = new List<string>();
                alreadyKnownDestinations.Add(dbId, destinationIds);
            }

            var destinationDbIds = new Dictionary<ReplicationDestination,string>();
            foreach (var destination in replicationDestinations)
            {
                var nodeDbId = GetDbIdFrom(destination);
                destinationDbIds.Add(destination,nodeDbId);
                alreadyKnownDestinations[dbId].Add(nodeDbId);
            }

            foreach (var destination in replicationDestinations)
            {                                
                var credentials = new OperationCredentials(destination.ApiKey,CredentialCache.DefaultCredentials);

                JsonOperationContext context;
                _disposables.Add(database.DocumentsStorage.ContextPool.AllocateOperationContext(out context));

                var alreadyKnownExceptCurrent = alreadyKnownDestinations.ToDictionary(x => x.Key, x => x.Value);
                alreadyKnownExceptCurrent[dbId].Remove(destinationDbIds[destination]);
                var singleDestinationDiscoverer = new ReplicationTopologyDestinationExplorer(
                    context,
                    alreadyKnownExceptCurrent,
                    destination,
                    credentials,
                    dbId,
                    _timeout);

                _discoverers.Add(singleDestinationDiscoverer);
            }
        }

        public async Task<FullTopologyInfo> DiscoverTopologyAsync()
        {
            if (_discoverers.Count == 0) //either no destinations or we already visited all destinations
                return new FullTopologyInfo(_database.DbId.ToString());

            var topology = new FullTopologyInfo(_database.DbId.ToString());
            var discoveryTasks = new Dictionary<ReplicationTopologyDestinationExplorer, Task<FullTopologyInfo>>(_discoverers.Count);
            foreach (var d in _discoverers)
            {
                Task<FullTopologyInfo> discoveryTask = null;
                try
                {
                    discoveryTask = d.DiscoverTopologyAsync();
                    discoveryTasks.Add(d, discoveryTask);
                }
                catch (Exception)
                {
                    //var val = CreateTopologyInfoFromFaultedDiscoveryTask(d, discoveryTask, ActiveNodeStatus.Status.Error);
                    //topology.NodesByDbId[d.Destination.Database] = val;
                }
            }

            var timedOut = false;
            try
            {
                var timeout = Task.Delay(_timeout);
                timedOut = await Task.WhenAny(timeout, Task.WhenAll(discoveryTasks.Values)) == timeout;
            }
            catch (Exception)
            {
                // handled externally
            }

            foreach (var kvp in discoveryTasks)
            {                
                var discoveryTask = kvp.Value;
                if (timedOut && 
                    discoveryTask.IsCompleted == false && 
                    discoveryTask.IsFaulted == false && 
                    discoveryTask.IsCanceled == false)
                {
                    ObserveTaskException(discoveryTask);
                    var topologyInfo = CreateTopologyInfoFromFaultedDiscoveryTask(kvp.Key, discoveryTask,ActiveNodeStatus.Status.Timeout);
                    topology.NodesByDbId[topologyInfo.OriginDbId] = topologyInfo;
                }
                else if (discoveryTask.IsFaulted || discoveryTask.IsCanceled)
                {
                    var topologyInfo = CreateTopologyInfoFromFaultedDiscoveryTask(kvp.Key, discoveryTask,ActiveNodeStatus.Status.Error);
                    topology.NodesByDbId[topologyInfo.OriginDbId] = topologyInfo;
                }
                //if kvp.Value.Result == null --> already visited
                //if the NodeTopologyInfo is empty it is a valid result
                else if (kvp.Value.Result != null)
                {
                    foreach (var nodeValue in kvp.Value.Result.NodesByDbId)
                        topology.NodesByDbId[nodeValue.Key] = nodeValue.Value;
                }
            }

            var replicationDocument = _database.DocumentReplicationLoader.GetReplicationDocument();

            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var localTopology = ReplicationUtils.GetLocalTopology(_database, replicationDocument, context);
                topology.NodesByDbId[localTopology.OriginDbId] = localTopology;
            }
            return topology;
        }

        private NodeTopologyInfo CreateTopologyInfoFromFaultedDiscoveryTask(
            ReplicationTopologyDestinationExplorer topologyExplorer, 
            Task<FullTopologyInfo> discoveryTask,
            ActiveNodeStatus.Status status)
        {
            OutgoingReplicationHandler outgoingHandler;
            var topologyInfo = new NodeTopologyInfo();

            if (ReplicationUtils.TryGetActiveDestination(
                topologyExplorer.Destination,
                _database.DocumentReplicationLoader.OutgoingHandlers,
                out outgoingHandler))
            {
                topologyInfo.Outgoing.Add(
                    new ActiveNodeStatus
                    {
                        DbId = outgoingHandler.DestinationDbId,
                        IsOnline = true, 
                        LastHeartbeatTicks = outgoingHandler.LastHeartbeatTicks,
                        LastDocumentEtag = outgoingHandler._lastSentDocumentEtag,
                        LastIndexTransformerEtag = outgoingHandler._lastSentIndexOrTransformerEtag,
                        NodeStatus = status,
                        LastException = discoveryTask.Exception?.ExtractSingleInnerException().ToString()
                    });
            }
            else
            {
                topologyInfo.Offline.Add(
                    new InactiveNodeStatus
                    {
                        Exception = discoveryTask.Exception?.ExtractSingleInnerException().ToString(),
                        Database = topologyExplorer.Destination.Database,
                        Url = topologyExplorer.Destination.Url
                    });
            }
            return topologyInfo;
        }

        private static void ObserveTaskException(Task<FullTopologyInfo> t)
        {
            t.ContinueWith(done => GC.KeepAlive(done.Exception));
        }

        private readonly HttpJsonRequestFactory jsonRequestFactory = new HttpJsonRequestFactory(1);

        private string GetDbIdFrom(ReplicationDestination destination)
        {
            var url = $"{destination.Url}/databases/{destination.Database}/topology/dbid";
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(
                new CreateHttpJsonRequestParams(null, url,
                    HttpMethod.Get,
                    new OperationCredentials(destination.ApiKey, CredentialCache.DefaultCredentials),
                    new DocumentConvention())))
            {
                var dbIdJson = request.ReadResponseJson();
                return dbIdJson.Value<string>("DbId");
            }
        }

        public void Dispose()
        {
            foreach(var disposable in _disposables)
                disposable.Dispose();
            foreach (var explorer in _discoverers)
            {
                explorer.Dispose();
            }
        }
    }
}
