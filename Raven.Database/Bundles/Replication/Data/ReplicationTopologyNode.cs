using System;
using System.Collections.Generic;

using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;

namespace Raven.Database.Bundles.Replication.Data
{
	public class ReplicationTopologyRootNode : ReplicationTopologyNodeBase
	{
		public Guid ServerId { get; set; }

		public ReplicationTopologyRootNode(string serverUrl, Guid serverId)
		{
			ServerUrl = serverUrl;
			ServerId = serverId;
		}

        private static void HandleLink(ReplicationTopology topology, ReplicationTopologyNodeBase source, ReplicationTopologyNodeBase target)
        {
            topology.Servers.Add(source.ServerUrl);
            topology.Servers.Add(target.ServerUrl);

            ReplicationTopologyConnection connection = null;

            if (target is ReplicationTopologyDestinationNode)
            {
                var destinationNode = (ReplicationTopologyDestinationNode)target;
                // going to destination
                connection = topology.GetConnection(source.ServerUrl, target.ServerUrl) ?? new ReplicationTopologyConnection();
                connection.Source = source.ServerUrl;
                connection.Destination = target.ServerUrl;
                connection.ReplicationBehavior = destinationNode.ReplicationBehavior;
                connection.SourceToDestinationState = destinationNode.State;
                connection.SendServerId = destinationNode.SendServerId;
                connection.Errors = destinationNode.Errors;
            }
            else if (target is ReplicationTopologySourceNode)
            {
                // going to source
                var sourceNode = (ReplicationTopologySourceNode)target;
                connection = topology.GetConnection(target.ServerUrl, source.ServerUrl) ?? new ReplicationTopologyConnection();
                connection.Source = target.ServerUrl;
                connection.Destination = source.ServerUrl;
                connection.DestinationToSourceState = sourceNode.State;
                connection.StoredServerId = sourceNode.StoredServerId;
                connection.LastDocumentEtag = sourceNode.LastDocumentEtag;
                connection.LastAttachmentEtag = sourceNode.LastAttachmentEtag;
            }

            topology.Connections.Add(connection);
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

		    return topology;
		}
	}

	public class ReplicationTopologyDestinationNode : ReplicationTopologyNode
	{
		public Guid SendServerId { get; set; }

		public TransitiveReplicationOptions ReplicationBehavior { get; protected set; }

		public static ReplicationTopologyDestinationNode Online(string serverUrl, Guid serverId, TransitiveReplicationOptions replicationBehavior)
		{
			return new ReplicationTopologyDestinationNode
			{
				ServerUrl = serverUrl,
				ReplicationBehavior = replicationBehavior,
				State = ReplicatonNodeState.Online,
				SendServerId = serverId
			};
		}

		public static ReplicationTopologyDestinationNode Offline(string serverUrl, Guid serverId, TransitiveReplicationOptions replicationBehavior)
		{
			return new ReplicationTopologyDestinationNode
			{
				ServerUrl = serverUrl,
				ReplicationBehavior = replicationBehavior,
				State = ReplicatonNodeState.Offline,
				SendServerId = serverId
			};
		}

		public static ReplicationTopologyDestinationNode Disabled(string serverUrl, Guid serverId, TransitiveReplicationOptions replicationBehavior)
		{
			return new ReplicationTopologyDestinationNode
			{
				ServerUrl = serverUrl,
				ReplicationBehavior = replicationBehavior,
				State = ReplicatonNodeState.Disabled,
				SendServerId = serverId
			};
		}
	}

	public class ReplicationTopologySourceNode : ReplicationTopologyNode
	{
		public Guid StoredServerId { get; set; }

		public Etag LastAttachmentEtag { get; set; }

		public Etag LastDocumentEtag { get; set; }

		public static ReplicationTopologySourceNode Online(string serverUrl, Guid serverId, Etag lastDocumentEtag, Etag lastAttachmentEtag)
		{
			return new ReplicationTopologySourceNode
				   {
					   ServerUrl = serverUrl,
					   State = ReplicatonNodeState.Online,
					   LastDocumentEtag = lastDocumentEtag,
					   LastAttachmentEtag = lastAttachmentEtag,
					   StoredServerId = serverId
				   };
		}

		public static ReplicationTopologySourceNode Offline(string serverUrl, Guid serverId, Etag lastDocumentEtag, Etag lastAttachmentEtag)
		{
			return new ReplicationTopologySourceNode
			{
				ServerUrl = serverUrl,
				State = ReplicatonNodeState.Offline,
				LastDocumentEtag = lastDocumentEtag,
				LastAttachmentEtag = lastAttachmentEtag,
				StoredServerId = serverId
			};
		}
	}

	public abstract class ReplicationTopologyNode : ReplicationTopologyNodeBase
	{
		public ReplicatonNodeState State { get; protected set; }
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

		public List<ReplicationTopologySourceNode> Sources { get; set; }

		public List<ReplicationTopologyDestinationNode> Destinations { get; set; }

		public List<string> Errors { get; set; }
	}
}