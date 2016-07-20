using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Bundles.Replication.Data;

namespace Raven.Database.FileSystem.Synchronization
{
    public class SynchronizationTopologyRootNode : SynchronizationTopologyNodeBase
    {
        private readonly Dictionary<Guid, string> resourceIdToUrl = new Dictionary<Guid, string>();
        private readonly Dictionary<string, Guid> urlToResourceId = new Dictionary<string, Guid>();

        public Guid ServerId { get; set; }

        public SynchronizationTopologyRootNode(string serverUrl, Guid serverId)
        {
            ServerUrl = serverUrl;
            ServerId = serverId;
        }

        private void HandleLink(SynchronizationTopology topology, SynchronizationTopologyNodeBase source, SynchronizationTopologyNodeBase target)
        {
            SynchronizationTopologyConnection connection = null;

            if (target is SynchronizationTopologyDestinationNode)
            {
                var destinationNode = (SynchronizationTopologyDestinationNode)target;
                // going to destination

                var sourceServerId = destinationNode.SendServerId;
                var sourceUrl = AddToCache(sourceServerId, source.ServerUrl);

                var destinationId = GetResourceId(destinationNode.DestinationServerId, destinationNode.ServerUrl);
                var targetUrl = AddToCache(destinationId, destinationNode.ServerUrl);

                connection = topology.GetConnection(sourceServerId, destinationId, sourceUrl, targetUrl);
                if (connection == null)
                {
                    connection = new SynchronizationTopologyConnection();
                    topology.Connections.Add(connection);
                }

                connection.SendServerId = destinationNode.SendServerId;
                connection.DestinationServerId = destinationId;
                connection.SourceUrl.Add(source.ServerUrl);
                connection.DestinationUrl.Add(target.ServerUrl);
                connection.SourceToDestinationState = destinationNode.State;
                connection.Errors = destinationNode.Errors;

                //left for backward compatibility with v3.0
                connection.Source = sourceUrl;
                connection.Destination = targetUrl;
            }
            else if (target is SynchronizationTopologySourceNode)
            {
                // going to source
                var sourceNode = (SynchronizationTopologySourceNode)target;
                var sourceServerId = sourceNode.StoredServerId;
                var sourceUrl = AddToCache(sourceServerId, target.ServerUrl);

                var destinationId = GetResourceId(sourceNode.DestinationServerId, source.ServerUrl);
                var targetUrl = AddToCache(destinationId, source.ServerUrl);

                connection = topology.GetConnection(sourceServerId, destinationId, sourceUrl, targetUrl);
                if (connection == null)
                {
                    connection = new SynchronizationTopologyConnection();
                    topology.Connections.Add(connection);
                }

                connection.StoredServerId = sourceServerId;
                connection.DestinationServerId = destinationId;
                connection.SourceUrl.Add(target.ServerUrl);
                connection.DestinationUrl.Add(source.ServerUrl);
                connection.DestinationToSourceState = sourceNode.State;
                connection.LastSourceFileEtag = sourceNode.LastSourceFileEtag;

                //left for backward compatibility with v3.0
                connection.Source = sourceUrl;
                connection.Destination = targetUrl;
            }

            topology.Connections.Add(connection);
        }

        private string AddToCache(Guid? resourceId, string url)
        {
            if (resourceId.HasValue == false)
                return url;

            if (urlToResourceId.ContainsKey(url) == false)
                urlToResourceId.Add(url, resourceId.Value);

            if (resourceIdToUrl.ContainsKey(resourceId.Value))
                return resourceIdToUrl[resourceId.Value];

            resourceIdToUrl.Add(resourceId.Value, url);

            return url;
        }

        private Guid? GetResourceId(Guid? resourceId, string url)
        {
            if (resourceId.HasValue)
                return resourceId.Value;

            Guid value;
            if (urlToResourceId.TryGetValue(url, out value))
                return value;

            return null;
        }

        private string GetUrlByResourceId(Guid? resourceId, string url)
        {
            Guid localResourceId;
            if (resourceId.HasValue)
            {
                localResourceId = resourceId.Value;
            }
            else
            {
                if (urlToResourceId.TryGetValue(url, out localResourceId) == false)
                    return url;
            }

            string value;
            return resourceIdToUrl.TryGetValue(localResourceId, out value) ? value : url;
        }

        public SynchronizationTopology Flatten()
        {
            var topology = new SynchronizationTopology();
            topology.Servers.Add(ServerUrl);

            var queue = new Queue<SynchronizationTopologyNodeBase>();
            queue.Enqueue(this);

            while (queue.Count > 0)
            {
                var node = queue.Dequeue();

                foreach (var dst in node.Destinations)
                {
                    HandleLink(topology, node, dst);
                    queue.Enqueue(dst);
                }

                foreach (var source in node.Sources)
                {
                    HandleLink(topology, node, source);
                    queue.Enqueue(source);
                }
            }

            foreach (var connection in topology.Connections)
            {
                connection.Source = GetUrlByResourceId(connection.SourceServerId, connection.Source);
                connection.Destination = GetUrlByResourceId(connection.DestinationServerId, connection.Destination);

                Guid resourceId;
                if (connection.DestinationServerId.HasValue == false &&
                    urlToResourceId.TryGetValue(connection.Destination, out resourceId))
                {
                    connection.DestinationServerId = resourceId;
                }

                topology.Servers.Add(connection.Source);
                topology.Servers.Add(connection.Destination);
            }

            return topology;
        }
    }

    public class SynchronizationTopologyDestinationNode : SynchronizationTopologyNode
    {
        public Guid SendServerId { get; set; }

        public static SynchronizationTopologyDestinationNode Online(
            string serverUrl, Guid sourceServerId, Guid? destinationServerId)
        {
            return new SynchronizationTopologyDestinationNode
            {
                ServerUrl = serverUrl,
                State = ReplicatonNodeState.Online,
                SendServerId = sourceServerId,
                DestinationServerId = destinationServerId
            };
        }

        public static SynchronizationTopologyDestinationNode Offline(
            string serverUrl, Guid sourceServerId, Guid? destinationServerId)
        {
            return new SynchronizationTopologyDestinationNode
            {
                ServerUrl = serverUrl,
                State = ReplicatonNodeState.Offline,
                SendServerId = sourceServerId,
                DestinationServerId = destinationServerId
            };
        }

        public static SynchronizationTopologyDestinationNode Disabled(
            string serverUrl, Guid sourceServerId, Guid? destinationServerId)
        {
            return new SynchronizationTopologyDestinationNode
            {
                ServerUrl = serverUrl,
                State = ReplicatonNodeState.Disabled,
                SendServerId = sourceServerId,
                DestinationServerId = destinationServerId
            };
        }
    }

    public class SynchronizationTopologySourceNode : SynchronizationTopologyNode
    {
        public Guid StoredServerId { get; set; }

        public Etag LastSourceFileEtag { get; set; }

        public static SynchronizationTopologySourceNode Online(string serverUrl, 
            Guid sourceServerId, Guid? destinationServerId, Etag lastSourceFileEtag)
        {
            return new SynchronizationTopologySourceNode
            {
                ServerUrl = serverUrl,
                State = ReplicatonNodeState.Online,
                LastSourceFileEtag = lastSourceFileEtag,
                StoredServerId = sourceServerId,
                DestinationServerId = destinationServerId
            };
        }

        public static SynchronizationTopologySourceNode Offline(string serverUrl, 
            Guid sourceServerId, Guid? destinationServerId, Etag lastSourceFileEtag)
        {
            return new SynchronizationTopologySourceNode
            {
                ServerUrl = serverUrl,
                State = ReplicatonNodeState.Offline,
                LastSourceFileEtag = lastSourceFileEtag,
                StoredServerId = sourceServerId,
                DestinationServerId = destinationServerId
            };
        }
    }

    public abstract class SynchronizationTopologyNode : SynchronizationTopologyNodeBase
    {
        public ReplicatonNodeState State { get; protected set; }

        public Guid? DestinationServerId { get; set; }
    }

    public abstract class SynchronizationTopologyNodeBase
    {
        protected SynchronizationTopologyNodeBase()
        {
            Sources = new List<SynchronizationTopologySourceNode>();
            Destinations = new List<SynchronizationTopologyDestinationNode>();
            Errors = new List<string>();
        }

        public string ServerUrl { get; protected set; }

        public List<SynchronizationTopologySourceNode> Sources { get; set; }

        public List<SynchronizationTopologyDestinationNode> Destinations { get; set; }

        public List<string> Errors { get; set; }
    }
}
