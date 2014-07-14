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

		public ReplicationTopology Flatten()
		{
			var topology = new ReplicationTopology();
			topology.Servers.Add(ServerUrl);

			foreach (var destination in Destinations)
			{
				AddRootConnection(topology, this, destination);
				foreach (var dst in destination.Destinations)
				{
					HandleDestination(topology, destination, dst);
				}

				foreach (var src in destination.Sources)
				{
					HandleSource(topology, destination, src);
				}
			}

			foreach (var source in Sources)
			{
				AddRootConnection(topology, this, source);
				foreach (var dst in source.Destinations)
				{
					HandleDestination(topology, source, dst);
				}

				foreach (var src in source.Sources)
				{
					HandleSource(topology, source, src);
				}
			}

			return topology;
		}

		private static void HandleSource(ReplicationTopology topology, ReplicationTopologyDestinationNode to, ReplicationTopologySourceNode from)
		{
			topology.Servers.Add(from.ServerUrl);
			topology.Servers.Add(to.ServerUrl);

			var connection = topology.GetConnection(@from.ServerUrl, to.ServerUrl) ?? new ReplicationTopologyConnection();
			connection.Source = from.ServerUrl;
			connection.Destination = to.ServerUrl;
			connection.StoredServerId = from.StoredServerId;
			connection.LastAttachmentEtag = from.LastAttachmentEtag;
			connection.LastDocumentEtag = from.LastDocumentEtag;
			connection.DestinationToSourceState = from.State;

			topology.Connections.Add(connection);

			foreach (var destination in from.Destinations)
			{
				HandleDestination(topology, from, destination);
			}

			foreach (var source in from.Sources)
			{
				HandleSource(topology, from, source);
			}
		}

		private static void HandleSource(ReplicationTopology topology, ReplicationTopologySourceNode to, ReplicationTopologySourceNode from)
		{
			topology.Servers.Add(from.ServerUrl);
			topology.Servers.Add(to.ServerUrl);

			var connection = topology.GetConnection(@from.ServerUrl, to.ServerUrl) ?? new ReplicationTopologyConnection();
			connection.Source = from.ServerUrl;
			connection.Destination = to.ServerUrl;
			connection.StoredServerId = from.StoredServerId;
			connection.LastAttachmentEtag = from.LastAttachmentEtag;
			connection.LastDocumentEtag = from.LastDocumentEtag;
			connection.DestinationToSourceState = from.State;

			topology.Connections.Add(connection);

			foreach (var destination in from.Destinations)
			{
				HandleDestination(topology, from, destination);
			}

			foreach (var source in from.Sources)
			{
				HandleSource(topology, from, source);
			}
		}

		private static void HandleDestination(ReplicationTopology topology, ReplicationTopologyDestinationNode from, ReplicationTopologyDestinationNode to)
		{
			topology.Servers.Add(from.ServerUrl);
			topology.Servers.Add(to.ServerUrl);

			var connection = topology.GetConnection(@from.ServerUrl, to.ServerUrl) ?? new ReplicationTopologyConnection();
			connection.Source = @from.ServerUrl;
			connection.Destination = to.ServerUrl;
			connection.SendServerId = to.SendServerId;
			connection.ReplicationBehavior = to.ReplicationBehavior;
			connection.Errors = to.Errors;
			connection.SourceToDestinationState = to.State;

			topology.Connections.Add(connection);

			foreach (var destination in to.Destinations)
			{
				HandleDestination(topology, to, destination);
			}

			foreach (var source in to.Sources)
			{
				HandleSource(topology, to, source);
			}
		}

		private static void HandleDestination(ReplicationTopology topology, ReplicationTopologySourceNode to, ReplicationTopologyDestinationNode from)
		{
			topology.Servers.Add(from.ServerUrl);
			topology.Servers.Add(to.ServerUrl);

			var connection = topology.GetConnection(@from.ServerUrl, to.ServerUrl) ?? new ReplicationTopologyConnection();
			connection.Source = @from.ServerUrl;
			connection.Destination = to.ServerUrl;
			connection.StoredServerId = to.StoredServerId;
			connection.LastAttachmentEtag = to.LastAttachmentEtag;
			connection.LastDocumentEtag = to.LastDocumentEtag;
			connection.Errors = to.Errors;
			connection.SourceToDestinationState = from.State;

			topology.Connections.Add(connection);

			foreach (var destination in to.Destinations)
			{
				HandleDestination(topology, to, destination);
			}

			foreach (var source in to.Sources)
			{
				HandleSource(topology, to, source);
			}
		}

		private static void AddRootConnection(ReplicationTopology topology, ReplicationTopologyRootNode root, ReplicationTopologyDestinationNode destination)
		{
			topology.Servers.Add(root.ServerUrl);
			topology.Servers.Add(destination.ServerUrl);

			var connection = topology.GetConnection(root.ServerUrl, destination.ServerUrl) ?? new ReplicationTopologyConnection();
			connection.Source = root.ServerUrl;
			connection.Destination = destination.ServerUrl;
			connection.SendServerId = destination.SendServerId;
			connection.ReplicationBehavior = destination.ReplicationBehavior;
			connection.Errors = destination.Errors;
			connection.SourceToDestinationState = destination.State;

			topology.Connections.Add(connection);
		}

		private static void AddRootConnection(ReplicationTopology topology, ReplicationTopologyRootNode root, ReplicationTopologySourceNode source)
		{
			topology.Servers.Add(root.ServerUrl);
			topology.Servers.Add(source.ServerUrl);

			var connection = topology.GetConnection(source.ServerUrl, root.ServerUrl) ?? new ReplicationTopologyConnection();
			connection.Source = source.ServerUrl;
			connection.Destination = root.ServerUrl;
			connection.LastAttachmentEtag = source.LastAttachmentEtag;
			connection.LastDocumentEtag = source.LastDocumentEtag;
			connection.StoredServerId = source.StoredServerId;
			connection.Errors = source.Errors;
			connection.DestinationToSourceState = source.State;

			topology.Connections.Add(connection);
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