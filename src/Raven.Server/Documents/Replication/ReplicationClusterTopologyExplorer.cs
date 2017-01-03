using System;
using System.Collections.Generic;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Replication;
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
        private readonly long _timeout;
        private readonly List<ReplicationTopologyDestinationExplorer> _discoverers;
        private readonly List<IDisposable> _disposables = new List<IDisposable>();
        public int DiscovererCount => _discoverers.Count;

        public ReplicationClusterTopologyExplorer(
            DocumentDatabase database,
            Dictionary<string, List<string>> alreadyKnownDestinations,
            long timeout, 
            List<ReplicationDestination> replicationDestinations)
        {
            _database = database;
            _timeout = timeout;
            _discoverers = new List<ReplicationTopologyDestinationExplorer>();
            var dbId = _database.DbId.ToString();
            foreach (var destination in replicationDestinations)
            {                                
                var credentials = new OperationCredentials(destination.ApiKey,CredentialCache.DefaultCredentials);

                JsonOperationContext context;
                _disposables.Add(database.DocumentsStorage.ContextPool.AllocateOperationContext(out context));

                var singleDestinationDiscoverer = new ReplicationTopologyDestinationExplorer(
                    context,
                    alreadyKnownDestinations,
                    destination,
                    credentials,
                    dbId,
                    _timeout);

                _discoverers.Add(singleDestinationDiscoverer);
            }
        }

        public async Task<List<NodeTopologyInfo>> DiscoverTopologyAsync()
        {
            if (_discoverers.Count == 0) //either no destinations or we already visited all destinations
                return new List<NodeTopologyInfo>();
            var discoveryTasks = new Dictionary<ReplicationTopologyDestinationExplorer, Task<NodeTopologyInfo>>(_discoverers.Count);
            foreach (var d in _discoverers)
                //TODO: this might throw immediately
                discoveryTasks.Add(d, d.DiscoverTopologyAsync());

            var timedOut = false;

            try
            {
                var timeout = Task.Delay(TimeSpan.FromMilliseconds(_timeout));
                timedOut = await Task.WhenAny(timeout, Task.WhenAll(discoveryTasks.Values)) == timeout;
            }
            catch (Exception)
            {
                // handled externally
            }

            var nodes = new List<NodeTopologyInfo>();
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
                    nodes.Add(topologyInfo);
                }
                else if (discoveryTask.IsFaulted || discoveryTask.IsCanceled)
                {
                    var topologyInfo = CreateTopologyInfoFromFaultedDiscoveryTask(kvp.Key, discoveryTask,ActiveNodeStatus.Status.Error);
                    nodes.Add(topologyInfo);
                }
                //if kvp.Value.Result == null --> already visited
                //if IsEmpty() == true --> leaf node
                else if (kvp.Value.Result != null && !IsEmpty(kvp.Value.Result)) 
                    nodes.Add(kvp.Value.Result);
            }

            var replicationDocument = _database.DocumentReplicationLoader.GetReplicationDocument();

            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                List<ReplicationDestination> activeDestinations;
                var localTopology = ReplicationUtils.GetLocalTopology(_database, replicationDocument, context, out activeDestinations);
                nodes.Add(localTopology);
            }
            return nodes;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsEmpty(NodeTopologyInfo topology)
        {
            return topology.IncomingByIncomingDbId.Count == 0 &&
                   topology.OutgoingByDbId.Count == 0 &&
                   topology.OfflineByUrlAndDatabase.Count == 0;
        }

        private NodeTopologyInfo CreateTopologyInfoFromFaultedDiscoveryTask(
            ReplicationTopologyDestinationExplorer topologyExplorer, 
            Task<NodeTopologyInfo> discoveryTask,
            ActiveNodeStatus.Status status)
        {
            OutgoingReplicationHandler outgoingHandler;
            var topologyInfo = new NodeTopologyInfo();

            if (ReplicationUtils.TryGetActiveDestination(
                topologyExplorer.Destination,
                _database.DocumentReplicationLoader.OutgoingHandlers,
                out outgoingHandler))
            {
                topologyInfo.OutgoingByDbId.Add(
                    _database.DbId.ToString(),
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
                topologyInfo.OfflineByUrlAndDatabase.Add(
                    $"{topologyExplorer.Destination.Url.ToLowerInvariant()}|{topologyExplorer.Destination.Database.ToLowerInvariant()}",
                    new InactiveNodeStatus
                    {
                        Exception = discoveryTask.Exception?.ExtractSingleInnerException().ToString(),
                        Database = topologyExplorer.Destination.Database,
                        Url = topologyExplorer.Destination.Url
                    });
            }
            return topologyInfo;
        }

        private static void ObserveTaskException(Task<NodeTopologyInfo> t)
        {
            t.ContinueWith(done => GC.KeepAlive(done.Exception));
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
