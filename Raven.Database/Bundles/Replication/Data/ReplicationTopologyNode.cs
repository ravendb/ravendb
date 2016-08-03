using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;

namespace Raven.Database.Bundles.Replication.Data
{
    public class ReplicationTopologyRootNode : ReplicationTopologyNodeBase
    {
        private readonly Dictionary<Guid, string> resourceIdToUrl = new Dictionary<Guid, string>();
        private readonly Dictionary<string, Guid> urlToResourceId = new Dictionary<string, Guid>();
         
        public ReplicationTopologyRootNode(string serverUrl, Guid serverId)
        {
            ServerUrl = serverUrl;
            ServerId = serverId;
        }

        private void HandleLink(ReplicationTopology topology, ReplicationTopologyNodeBase source, ReplicationTopologyNodeBase target)
        {
            ReplicationTopologyConnection connection = null;

            if (target is ReplicationTopologyDestinationNode)
            {
                var destinationNode = (ReplicationTopologyDestinationNode)target;
                // going to destination

                var sourceServerId = destinationNode.SendServerId;
                var sourceUrl = AddToCache(sourceServerId, source.ServerUrl);

                var destinationId = GetResourceId(destinationNode.DestinationServerId, destinationNode.ServerUrl);
                var targetUrl = AddToCache(destinationId, destinationNode.ServerUrl);

                connection = topology.GetConnection(sourceServerId, destinationId, sourceUrl, targetUrl);
                if (connection == null)
                {
                    connection = new ReplicationTopologyConnection();
                    topology.Connections.Add(connection);
                }

                connection.SendServerId = sourceServerId;
                connection.DestinationServerId = destinationId;
                connection.SourceUrl.Add(source.ServerUrl);
                connection.DestinationUrl.Add(target.ServerUrl);
                connection.ReplicationBehavior = destinationNode.ReplicationBehavior;
                connection.SourceToDestinationState = destinationNode.State;
                connection.Errors = destinationNode.Errors;

                //left for backward compatibility with v3.0
                connection.Source = sourceUrl;
                connection.Destination = targetUrl;
            }
            else if (target is ReplicationTopologySourceNode)
            {
                // going to source
                var sourceNode = (ReplicationTopologySourceNode)target;
                var sourceServerId = sourceNode.StoredServerId;
                var sourceUrl = AddToCache(sourceServerId, target.ServerUrl);

                var destinationId = GetResourceId(sourceNode.DestinationServerId, source.ServerUrl);
                var targetUrl = AddToCache(destinationId, source.ServerUrl);

                connection = topology.GetConnection(sourceServerId, destinationId, sourceUrl, targetUrl);
                if (connection == null)
                {
                    connection = new ReplicationTopologyConnection();
                    topology.Connections.Add(connection);
                }

                connection.StoredServerId = sourceServerId;
                connection.DestinationServerId = destinationId;
                connection.SourceUrl.Add(target.ServerUrl);
                connection.DestinationUrl.Add(source.ServerUrl);
                connection.DestinationToSourceState = sourceNode.State;
                connection.LastDocumentEtag = sourceNode.LastDocumentEtag;
                connection.LastAttachmentEtag = sourceNode.LastAttachmentEtag;

                //left for backward compatibility with v3.0
                connection.Source = sourceUrl;
                connection.Destination = targetUrl;
            }
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

        private string GetUrlByDatabaseId(Guid? databaseId, string url)
        {
            Guid localResourceId;
            if (databaseId.HasValue)
            {
                localResourceId = databaseId.Value;
            }
            else
            {
                if (urlToResourceId.TryGetValue(url, out localResourceId) == false)
                    return url;
            }

            string value;
            return resourceIdToUrl.TryGetValue(localResourceId, out value) ? value : url;
        }

        public ReplicationTopology Flatten()
        {
            var topology = new ReplicationTopology();
            topology.Servers.Add(ServerUrl);

            var queue = new Queue<ReplicationTopologyNodeBase>();
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
                connection.Source = GetUrlByDatabaseId(connection.SourceServerId, connection.Source);
                connection.Destination = GetUrlByDatabaseId(connection.DestinationServerId, connection.Destination);

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

    public class ReplicationTopologyDestinationNode : ReplicationTopologyNode
    {
        public Guid SendServerId { get; set; }

        public TransitiveReplicationOptions ReplicationBehavior { get; protected set; }

        public static ReplicationTopologyDestinationNode Online(string serverUrl, 
            Guid sourceServerId, Guid? destinationServerId, TransitiveReplicationOptions replicationBehavior)
        {
            return new ReplicationTopologyDestinationNode
            {
                ServerUrl = serverUrl,
                ReplicationBehavior = replicationBehavior,
                State = ReplicatonNodeState.Online,
                SendServerId = sourceServerId,
                DestinationServerId = destinationServerId
            };
        }

        public static ReplicationTopologyDestinationNode Offline(string serverUrl, 
            Guid sourceServerId, Guid? destinationServerId, TransitiveReplicationOptions replicationBehavior)
        {
            return new ReplicationTopologyDestinationNode
            {
                ServerUrl = serverUrl,
                ReplicationBehavior = replicationBehavior,
                State = ReplicatonNodeState.Offline,
                SendServerId = sourceServerId,
                DestinationServerId = destinationServerId
            };
        }

        public static ReplicationTopologyDestinationNode Disabled(string serverUrl, 
            Guid sourceServerId, Guid? destinationServerId, TransitiveReplicationOptions replicationBehavior)
        {
            return new ReplicationTopologyDestinationNode
            {
                ServerUrl = serverUrl,
                ReplicationBehavior = replicationBehavior,
                State = ReplicatonNodeState.Disabled,
                SendServerId = sourceServerId,
                DestinationServerId = destinationServerId,
            };
        }
    }

    public class ReplicationTopologySourceNode : ReplicationTopologyNode
    {
        public Guid StoredServerId { get; set; }

        [Obsolete("Use RavenFS instead.")]
        public Etag LastAttachmentEtag { get; set; }

        public Etag LastDocumentEtag { get; set; }

        public static ReplicationTopologySourceNode Online(string serverUrl, 
            Guid sourceServerId, Guid destinationServerId, Etag lastDocumentEtag, Etag lastAttachmentEtag)
        {
            return new ReplicationTopologySourceNode
            {
                ServerUrl = serverUrl,
                State = ReplicatonNodeState.Online,
                LastDocumentEtag = lastDocumentEtag,
                LastAttachmentEtag = lastAttachmentEtag,
                StoredServerId = sourceServerId,
                DestinationServerId = destinationServerId,
            };
        }

        public static ReplicationTopologySourceNode Offline(string serverUrl,
            Guid sourceServerId, Guid destinationServerId, Etag lastDocumentEtag, Etag lastAttachmentEtag)
        {
            return new ReplicationTopologySourceNode
            {
                ServerUrl = serverUrl,
                State = ReplicatonNodeState.Offline,
                LastDocumentEtag = lastDocumentEtag,
                LastAttachmentEtag = lastAttachmentEtag,
                StoredServerId = sourceServerId,
                DestinationServerId = destinationServerId,
            };
        }
    }

    public abstract class ReplicationTopologyNode : ReplicationTopologyNodeBase
    {
        public ReplicatonNodeState State { get; protected set; }

        public Guid? DestinationServerId { get; set; }
    }

    public abstract class ReplicationTopologyNodeBase
    {
        protected ReplicationTopologyNodeBase()
        {
            Sources = new List<ReplicationTopologySourceNode>();
            Destinations = new List<ReplicationTopologyDestinationNode>();
            Errors = new List<string>();
        }

        public string ServerUrl { get; protected set; }

        public Guid ServerId { get; set; }

        public List<ReplicationTopologySourceNode> Sources { get; set; }

        public List<ReplicationTopologyDestinationNode> Destinations { get; set; }

        public List<string> Errors { get; set; }
    }
}
