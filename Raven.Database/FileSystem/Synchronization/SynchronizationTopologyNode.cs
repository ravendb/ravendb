using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Database.Bundles.Replication.Data;

namespace Raven.Database.FileSystem.Synchronization
{
	public class SynchronizationTopologyRootNode : SynchronizationTopologyNodeBase
	{
		public Guid ServerId { get; set; }

		public SynchronizationTopologyRootNode(string serverUrl, Guid serverId)
		{
			ServerUrl = serverUrl;
			ServerId = serverId;
		}

        private static void HandleLink(SynchronizationTopology topology, SynchronizationTopologyNodeBase source, SynchronizationTopologyNodeBase target)
        {
            topology.Servers.Add(source.ServerUrl);
            topology.Servers.Add(target.ServerUrl);

			SynchronizationTopologyConnection connection = null;

            if (target is SynchronizationTopologyDestinationNode)
            {
                var destinationNode = (SynchronizationTopologyDestinationNode)target;
                // going to destination
                connection = topology.GetConnection(source.ServerUrl, target.ServerUrl) ?? new SynchronizationTopologyConnection();
                connection.Source = source.ServerUrl;
                connection.Destination = target.ServerUrl;
                connection.SourceToDestinationState = destinationNode.State;
                connection.SendServerId = destinationNode.SendServerId;
                connection.Errors = destinationNode.Errors;
            }
            else if (target is SynchronizationTopologySourceNode)
            {
                // going to source
                var sourceNode = (SynchronizationTopologySourceNode)target;
                connection = topology.GetConnection(target.ServerUrl, source.ServerUrl) ?? new SynchronizationTopologyConnection();
                connection.Source = target.ServerUrl;
                connection.Destination = source.ServerUrl;
				connection.DestinationToSourceState = sourceNode.State;
                connection.StoredServerId = sourceNode.StoredServerId;
                connection.LastSourceFileEtag = sourceNode.LastSourceFileEtag;
            }

            topology.Connections.Add(connection);
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

		    return topology;
		}
	}

	public class SynchronizationTopologyDestinationNode : SynchronizationTopologyNode
	{
		public Guid SendServerId { get; set; }

		public static SynchronizationTopologyDestinationNode Online(string serverUrl, Guid serverId)
		{
			return new SynchronizationTopologyDestinationNode
			{
				ServerUrl = serverUrl,
				State = ReplicatonNodeState.Online,
				SendServerId = serverId
			};
		}

		public static SynchronizationTopologyDestinationNode Offline(string serverUrl, Guid serverId)
		{
			return new SynchronizationTopologyDestinationNode
			{
				ServerUrl = serverUrl,
				State = ReplicatonNodeState.Offline,
				SendServerId = serverId
			};
		}

		public static SynchronizationTopologyDestinationNode Disabled(string serverUrl, Guid serverId)
		{
			return new SynchronizationTopologyDestinationNode
			{
				ServerUrl = serverUrl,
				State = ReplicatonNodeState.Disabled,
				SendServerId = serverId
			};
		}
	}

	public class SynchronizationTopologySourceNode : SynchronizationTopologyNode
	{
		public Guid StoredServerId { get; set; }

		public Etag LastSourceFileEtag { get; set; }

		public static SynchronizationTopologySourceNode Online(string serverUrl, Guid serverId, Etag lastSourceFileEtag)
		{
			return new SynchronizationTopologySourceNode
			{
					   ServerUrl = serverUrl,
					   State = ReplicatonNodeState.Online,
					   LastSourceFileEtag = lastSourceFileEtag,
					   StoredServerId = serverId
				   };
		}

		public static SynchronizationTopologySourceNode Offline(string serverUrl, Guid serverId, Etag lastSourceFileEtag)
		{
			return new SynchronizationTopologySourceNode
			{
				ServerUrl = serverUrl,
				State = ReplicatonNodeState.Offline,
				LastSourceFileEtag = lastSourceFileEtag,
				StoredServerId = serverId
			};
		}
	}

	public abstract class SynchronizationTopologyNode : SynchronizationTopologyNodeBase
	{
		public ReplicatonNodeState State { get; protected set; }
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