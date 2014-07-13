using System;
using System.Collections.Generic;

using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;

namespace Raven.Database.Bundles.Replication.Data
{
	public class ReplicationSchemaRootNode : ReplicationSchemaNodeBase
	{
		public Guid ServerId { get; set; }

		public ReplicationSchemaRootNode(string serverUrl, Guid serverId)
		{
			ServerUrl = serverUrl;
			ServerId = serverId;
		}
	}

	public class ReplicationSchemaDestinationNode : ReplicationSchemaNode
	{
		public Guid SendServerId { get; set; }

		public TransitiveReplicationOptions ReplicationBehavior { get; protected set; }

		public static ReplicationSchemaDestinationNode Online(string serverUrl, Guid serverId, TransitiveReplicationOptions replicationBehavior)
		{
			return new ReplicationSchemaDestinationNode
			{
				ServerUrl = serverUrl,
				ReplicationBehavior = replicationBehavior,
				State = ReplicatonNodeState.Online,
				SendServerId = serverId
			};
		}

		public static ReplicationSchemaDestinationNode Offline(string serverUrl, Guid serverId, TransitiveReplicationOptions replicationBehavior)
		{
			return new ReplicationSchemaDestinationNode
			{
				ServerUrl = serverUrl,
				ReplicationBehavior = replicationBehavior,
				State = ReplicatonNodeState.Offline,
				SendServerId = serverId
			};
		}

		public static ReplicationSchemaDestinationNode Disabled(string serverUrl, Guid serverId, TransitiveReplicationOptions replicationBehavior)
		{
			return new ReplicationSchemaDestinationNode
			{
				ServerUrl = serverUrl,
				ReplicationBehavior = replicationBehavior,
				State = ReplicatonNodeState.Disabled,
				SendServerId = serverId
			};
		}
	}

	public class ReplicationSchemaSourceNode : ReplicationSchemaNode
	{
		public Guid StoredServerId { get; set; }

		public Etag LastAttachmentEtag { get; set; }

		public Etag LastDocumentEtag { get; set; }

		public static ReplicationSchemaSourceNode Online(string serverUrl, Guid serverId, Etag lastDocumentEtag, Etag lastAttachmentEtag)
		{
			return new ReplicationSchemaSourceNode
				   {
					   ServerUrl = serverUrl,
					   State = ReplicatonNodeState.Online,
					   LastDocumentEtag = lastDocumentEtag,
					   LastAttachmentEtag = lastAttachmentEtag,
					   StoredServerId = serverId
				   };
		}

		public static ReplicationSchemaSourceNode Offline(string serverUrl, Guid serverId, Etag lastDocumentEtag, Etag lastAttachmentEtag)
		{
			return new ReplicationSchemaSourceNode
			{
				ServerUrl = serverUrl,
				State = ReplicatonNodeState.Offline,
				LastDocumentEtag = lastDocumentEtag,
				LastAttachmentEtag = lastAttachmentEtag,
				StoredServerId = serverId
			};
		}
	}

	public abstract class ReplicationSchemaNode : ReplicationSchemaNodeBase
	{
		public ReplicatonNodeState State { get; protected set; }
	}

	public abstract class ReplicationSchemaNodeBase
	{
		protected ReplicationSchemaNodeBase()
		{
			Sources = new List<ReplicationSchemaSourceNode>();
			Destinations = new List<ReplicationSchemaDestinationNode>();
			Errors = new List<string>();
		}

		public string ServerUrl { get; protected set; }

		public List<ReplicationSchemaSourceNode> Sources { get; set; }

		public List<ReplicationSchemaDestinationNode> Destinations { get; set; }

		public List<string> Errors { get; set; }
	}
}