using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Data;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Bundles.Replication.Data;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Impl
{
	internal class ReplicationTopologyDiscoverer
	{
		private readonly DocumentDatabase database;

		private readonly int ttl;

		private readonly ILog log;

		private readonly RavenJArray @from;

		private readonly HttpRavenRequestFactory requestFactory;

		public ReplicationTopologyDiscoverer(DocumentDatabase database, RavenJArray @from, int ttl, ILog log)
		{
			this.database = database;
			this.ttl = ttl;
			this.log = log;
			this.@from = @from;
			requestFactory = new HttpRavenRequestFactory();
		}

		public ReplicationTopologyRootNode Discover()
		{
			var nextStart = 0;

			var root = new ReplicationTopologyRootNode(database.ServerUrl, database.TransactionalStorage.Id);

			if (ttl <= 0)
				return root;

			var destinations = database.Documents.Get(Constants.RavenReplicationDestinations, null);
			var sources = database.Documents.GetDocumentsWithIdStartingWith(Constants.RavenReplicationSourcesBasePath, null, null, 0, int.MaxValue, database.WorkContext.CancellationToken, ref nextStart);

            if (@from.Contains(database.ServerUrl) == false)
            {
                @from.Add(database.ServerUrl);
            }

			root.Destinations = HandleDestinations(destinations, root);
			root.Sources = HandleSources(sources, root);

			return root;
		}

		private List<ReplicationTopologySourceNode> HandleSources(IEnumerable<RavenJToken> sources, ReplicationTopologyRootNode root)
		{
			var nodes = new List<ReplicationTopologySourceNode>();
			foreach (var sourceAsJson in sources.Cast<RavenJObject>())
			{
				SourceReplicationInformation source = null;
				try
				{
					source = sourceAsJson.JsonDeserialization<SourceReplicationInformation>();
				}
				catch (Exception)
				{
					root.Errors.Add(string.Format("Could not deserialize source node."));
				}

				var node = HandleSource(source);
				nodes.Add(node);
			}

			return nodes;
		}

		private ReplicationTopologySourceNode HandleSource(SourceReplicationInformation source)
		{
			if (from.Contains(source.Source))
			{
				var state = CheckSourceConnectionState(source.Source);
				switch (state)
				{
					case ReplicatonNodeState.Online:
						return ReplicationTopologySourceNode.Online(source.Source, source.ServerInstanceId, source.LastDocumentEtag, source.LastAttachmentEtag);
					case ReplicatonNodeState.Offline:
						return ReplicationTopologySourceNode.Offline(source.Source, source.ServerInstanceId, source.LastDocumentEtag, source.LastAttachmentEtag);
					default:
						throw new NotSupportedException(state.ToString());
				}
			}

			string error;
			ReplicationTopologyRootNode rootNode;
			if (TryGetSchema(source.Source, new RavenConnectionStringOptions(), out rootNode, out error))
			{
				var node = ReplicationTopologySourceNode.Online(source.Source, source.ServerInstanceId, source.LastDocumentEtag, source.LastAttachmentEtag);
				node.Destinations = rootNode.Destinations;
				node.Sources = rootNode.Sources;
				node.Errors = rootNode.Errors;

				return node;
			}

			var offline = ReplicationTopologySourceNode.Online(source.Source, source.ServerInstanceId, source.LastDocumentEtag, source.LastAttachmentEtag);

			if (string.IsNullOrEmpty(error) == false)
				offline.Errors.Add(error);

			return offline;
		}

		private List<ReplicationTopologyDestinationNode> HandleDestinations(JsonDocument destinationsAsJson, ReplicationTopologyRootNode root)
		{
			var nodes = new List<ReplicationTopologyDestinationNode>();

			if (destinationsAsJson == null)
				return nodes;

			ReplicationDocument destinations;
			try
			{
				destinations = destinationsAsJson.DataAsJson.JsonDeserialization<ReplicationDocument>();
			}
			catch (Exception)
			{
				root.Errors.Add(string.Format("Could not deserialize '{0}'.", Constants.RavenReplicationDestinations));

				return nodes;
			}

			nodes.AddRange(destinations.Destinations.Select(HandleDestination));

			return nodes;
		}

		private ReplicationTopologyDestinationNode HandleDestination(ReplicationDestination replicationDestination)
		{
			var destination = ReplicationTask.GetConnectionOptions(replicationDestination, database);

			if (replicationDestination.Disabled)
				return ReplicationTopologyDestinationNode.Disabled(destination.ConnectionStringOptions.Url, database.TransactionalStorage.Id, destination.ReplicationOptionsBehavior);

			if (from.Contains(destination.ConnectionStringOptions.Url))
			{
				var state = CheckDestinationConnectionState(destination);
				switch (state)
				{
					case ReplicatonNodeState.Online:
						return ReplicationTopologyDestinationNode.Online(destination.ConnectionStringOptions.Url, database.TransactionalStorage.Id, destination.ReplicationOptionsBehavior);
					case ReplicatonNodeState.Offline:
						return ReplicationTopologyDestinationNode.Offline(destination.ConnectionStringOptions.Url, database.TransactionalStorage.Id, destination.ReplicationOptionsBehavior);
					default:
						throw new NotSupportedException(state.ToString());
				}
			}

			string error;
			ReplicationTopologyRootNode rootNode;
			if (TryGetSchema(destination.ConnectionStringOptions.Url, destination.ConnectionStringOptions, out rootNode, out error))
			{
				var node = ReplicationTopologyDestinationNode.Online(destination.ConnectionStringOptions.Url, database.TransactionalStorage.Id, destination.ReplicationOptionsBehavior);
				node.Destinations = rootNode.Destinations;
				node.Sources = rootNode.Sources;
				node.Errors = rootNode.Errors;

				return node;
			}

			var offline = ReplicationTopologyDestinationNode.Offline(destination.ConnectionStringOptions.Url, database.TransactionalStorage.Id, destination.ReplicationOptionsBehavior);

			if (string.IsNullOrEmpty(error) == false)
				offline.Errors.Add(error);

			return offline;
		}

		private bool TryGetSchema(string serverUrl, RavenConnectionStringOptions connectionStringOptions, out ReplicationTopologyRootNode rootNode, out string error)
		{
			var url = string.Format("{0}/admin/replication/topology/discover?&ttl={1}", serverUrl, ttl - 1);

			try
			{
				var request = requestFactory.Create(url, "POST", connectionStringOptions);
                request.Write(from);

				error = null;
				rootNode = request.ExecuteRequest<ReplicationTopologyRootNode>();

			    var visitedNodes = new HashSet<string>();
			    FindVisitedNodes(rootNode, visitedNodes);
                foreach (var visitedNode in visitedNodes)
                {
                    if (@from.Contains(visitedNode) == false)
                    {
                        @from.Add(visitedNode);
                    }
                }
				return true;
			}
			catch (Exception e)
			{
				error = e.Message;
				rootNode = null;
				return false;
			}
		}

        private void FindVisitedNodes(ReplicationTopologyNodeBase rootNode, HashSet<string> visitedNodes)
	    {
	        visitedNodes.Add(rootNode.ServerUrl);
	        foreach (var source in rootNode.Sources)
	        {
	            FindVisitedNodes(source, visitedNodes);
	        }
            foreach (var destinationNode in rootNode.Destinations)
            {
                FindVisitedNodes(destinationNode, visitedNodes);
            }
	    }

	    private ReplicatonNodeState CheckSourceConnectionState(string sourceUrl)
		{
			return CheckConnectionState(sourceUrl, new RavenConnectionStringOptions());
		}

		private ReplicatonNodeState CheckDestinationConnectionState(ReplicationStrategy destination)
		{
			return CheckConnectionState(destination.ConnectionStringOptions.Url, destination.ConnectionStringOptions);
		}

		private ReplicatonNodeState CheckConnectionState(string serverUrl, RavenConnectionStringOptions connectionStringOptions)
		{
			try
			{
				var url = string.Format("{0}/replication/heartbeat?&from={1}", serverUrl, Uri.EscapeDataString(database.ServerUrl));
				var request = requestFactory.Create(url, "POST", connectionStringOptions);
				request.ExecuteRequest();
			}
			catch (Exception e)
			{
				log.ErrorException(string.Format("Could not connect to '{0}'.", serverUrl), e);

				return ReplicatonNodeState.Offline;
			}

			return ReplicatonNodeState.Online;
		}
	}
}