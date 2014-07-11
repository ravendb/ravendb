using System;
using System.Collections.Generic;

using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;

namespace Raven.Database.Bundles.Replication.Data
{
	internal class ReplicationSchemaRootNode : ReplicationSchemaNodeBase
	{
		public ReplicationSchemaRootNode(string serverUrl, Guid serverId)
		{
			ServerUrl = serverUrl;
			ServerId = serverId;
		}
	}

	internal class ReplicationSchemaDestinationNode : ReplicationSchemaNode
	{
		public TransitiveReplicationOptions ReplicationBehavior { get; protected set; }

		public static ReplicationSchemaDestinationNode Online(string serverUrl, Guid serverId, TransitiveReplicationOptions replicationBehavior)
		{
			return new ReplicationSchemaDestinationNode
			{
				ServerUrl = serverUrl,
				ReplicationBehavior = replicationBehavior,
				State = ReplicatonNodeState.Online,
				ServerId = serverId
			};
		}

		public static ReplicationSchemaDestinationNode Offline(string serverUrl, Guid serverId, TransitiveReplicationOptions replicationBehavior)
		{
			return new ReplicationSchemaDestinationNode
			{
				ServerUrl = serverUrl,
				ReplicationBehavior = replicationBehavior,
				State = ReplicatonNodeState.Offline,
				ServerId = serverId
			};
		}

		public static ReplicationSchemaDestinationNode Disabled(string serverUrl, Guid serverId, TransitiveReplicationOptions replicationBehavior)
		{
			return new ReplicationSchemaDestinationNode
			{
				ServerUrl = serverUrl,
				ReplicationBehavior = replicationBehavior,
				State = ReplicatonNodeState.Disabled,
				ServerId = serverId
			};
		}
	}

	internal class ReplicationSchemaSourceNode : ReplicationSchemaNode
	{
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
					   ServerId = serverId
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
				ServerId = serverId
			};
		}
	}

	internal abstract class ReplicationSchemaNode : ReplicationSchemaNodeBase
	{
		public ReplicatonNodeState State { get; protected set; }
	}

	internal abstract class ReplicationSchemaNodeBase
	{
		protected ReplicationSchemaNodeBase()
		{
			Sources = new List<ReplicationSchemaSourceNode>();
			Destinations = new List<ReplicationSchemaDestinationNode>();
			Errors = new List<string>();
		}

		public string ServerUrl { get; protected set; }

		public Guid ServerId { get; set; }

		public List<ReplicationSchemaSourceNode> Sources { get; set; }

		public List<ReplicationSchemaDestinationNode> Destinations { get; set; }

		public List<string> Errors { get; set; }
	}
}