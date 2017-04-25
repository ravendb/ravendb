using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Extensions;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Replication
{
    public class ClusterTopologyExplorer : IDisposable
    {
        private readonly DocumentDatabase _database;
        private readonly TimeSpan _timeout;
        private readonly List<NodeTopologyExplorer> _discoverers;

        public ClusterTopologyExplorer(
            DocumentDatabase database,
            List<string> alreadyVisited,
            TimeSpan timeout,
            List<ReplicationNode> replicationDestinations)
        {
            _database = database;
            _timeout = timeout;
            _discoverers = new List<NodeTopologyExplorer>();
            var dbId = _database.DbId.ToString();

            foreach (var destination in replicationDestinations)
            {
                if (destination.Disabled)
                    continue;

                var singleDestinationDiscoverer = new NodeTopologyExplorer(
                    database.DocumentsStorage.ContextPool,
                    alreadyVisited,
                    destination,
                    dbId,
                    _timeout);

                _discoverers.Add(singleDestinationDiscoverer);
            }
        }

        public async Task<FullTopologyInfo> DiscoverTopologyAsync()
        {
            var topology = new FullTopologyInfo { DatabaseId = _database.DbId.ToString() };
            if (_discoverers.Count == 0) //either no destinations or we already visited all destinations
                return topology;

            var discoveryTasks =
                new Dictionary<NodeTopologyExplorer, Task<FullTopologyInfo>>(_discoverers.Count);
            foreach (var d in _discoverers)
            {
                try
                {
                    var discoveryTask = d.DiscoverTopologyAsync();
                    discoveryTasks.Add(d, discoveryTask);
                }
                catch (Exception e)
                {
                    topology.FailedToReach.Add(new InactiveNodeStatus
                    {
                        Database = d.Node.Database,
                        Url = d.Node.Url,
                        Exception = e.ToString(),
                        Message = e.Message
                    });
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
                    topology.FailedToReach.Add(new InactiveNodeStatus
                    {
                        Database = kvp.Key.Node.Database,
                        Url = kvp.Key.Node.Url,
                        Exception = $"Timed out trying to reach destination after {_timeout} ms"
                    });
                }
                else if (discoveryTask.IsFaulted || discoveryTask.IsCanceled)
                {
                    topology.FailedToReach.Add(new InactiveNodeStatus
                    {
                        Database = kvp.Key.Node.Database,
                        Url = kvp.Key.Node.Url,
                        Message = discoveryTask.Exception?.Message,
                        Exception = discoveryTask.Exception?.ExtractSingleInnerException().ToString()
                    });
                }
                else if (kvp.Value.Result != null)
                {
                    foreach (var nodeValue in kvp.Value.Result.NodesById)
                    {
                        topology.NodesById[nodeValue.Key] = nodeValue.Value;
                    }
                }
            }

            var localTopology = ReplicationUtils.GetLocalTopology(_database,
				_database.ReplicationLoader.Destinations);
            topology.NodesById[localTopology.DatabaseId] = localTopology;
            return topology;
        }

        private static void ObserveTaskException(Task<FullTopologyInfo> t)
        {
            t.ContinueWith(done => GC.KeepAlive(done.Exception));
        }

        public void Dispose()
        {
            foreach (var explorer in _discoverers)
            {
                explorer.Dispose();
            }
        }
    }
}
