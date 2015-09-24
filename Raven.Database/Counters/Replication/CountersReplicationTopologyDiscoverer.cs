using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Data;
using Raven.Client.Connection;
using Raven.Database.Bundles.Replication.Data;
using Raven.Json.Linq;

namespace Raven.Database.Counters.Replication
{
	internal class CountersReplicationTopologyDiscoverer
	{
		private readonly CounterStorage counterStorage;

		private readonly int ttl;

		private readonly ILog log;

		private readonly RavenJArray @from;

		private readonly HttpRavenRequestFactory requestFactory;

		public CountersReplicationTopologyDiscoverer(CounterStorage counterStorage, RavenJArray @from, int ttl, ILog log)
		{
			this.counterStorage = counterStorage;
			this.ttl = ttl;
			this.log = log;
			this.@from = @from;
			requestFactory = new HttpRavenRequestFactory();
		}

		public CountersReplicationTopologyRootNode Discover()
		{
			var root = new CountersReplicationTopologyRootNode(counterStorage.CounterStorageUrl, counterStorage.ServerId);

			if (ttl <= 0)
				return root;

			CountersReplicationDocument replicationData;
			IEnumerable<CounterStorage.ServerEtagAndSourceName> serverSources;

			using (var reader = counterStorage.CreateReader())
			{
				replicationData = reader.GetReplicationData();
				serverSources = reader.GetServerSources().ToList();
			}

            if (@from.Contains(counterStorage.CounterStorageUrl) == false)
            {
                @from.Add(counterStorage.CounterStorageUrl);
            }

			if (replicationData != null)
				root.Destinations = HandleDestinations(replicationData.Destinations);

			root.Sources = HandleSources(serverSources, root);

			return root;
		}

		private List<CountersReplicationTopologySourceNode> HandleSources(IEnumerable<CounterStorage.ServerEtagAndSourceName> serverSources, CountersReplicationTopologyRootNode root)
		{
			return serverSources
				.Select(HandleSource)
				.ToList();
		}

		private CountersReplicationTopologySourceNode HandleSource(CounterStorage.ServerEtagAndSourceName source)
		{
			if (from.Contains(source.SourceName))
			{
				var state = CheckSourceConnectionState(source.SourceName);
				switch (state)
				{
					case ReplicatonNodeState.Online:
						return CountersReplicationTopologySourceNode.Online(source.SourceName, source.ServerId, source.Etag);
					case ReplicatonNodeState.Offline:
						return CountersReplicationTopologySourceNode.Offline(source.SourceName, source.ServerId, source.Etag);
					default:
						throw new NotSupportedException(state.ToString());
				}
			}

			string error;
			CountersReplicationTopologyRootNode rootNode;
			if (TryGetSchema(source.SourceName, new RavenConnectionStringOptions(), out rootNode, out error))
			{
				var node = CountersReplicationTopologySourceNode.Online(source.SourceName, source.ServerId, source.Etag);
				node.Destinations = rootNode.Destinations;
				node.Sources = rootNode.Sources;
				node.Errors = rootNode.Errors;

				return node;
			}

			var offline = CountersReplicationTopologySourceNode.Online(source.SourceName, source.ServerId, source.Etag);

			if (string.IsNullOrEmpty(error) == false)
				offline.Errors.Add(error);

			return offline;
		}

		private List<CountersReplicationTopologyDestinationNode> HandleDestinations(List<CounterReplicationDestination> destinations)
		{
			return destinations
				.Select(HandleDestination)
				.ToList();
		}

		private CountersReplicationTopologyDestinationNode HandleDestination(CounterReplicationDestination replicationDestination)
		{
			RavenConnectionStringOptions connectionStringOptions = new RavenConnectionStringOptions
			{
				Credentials = replicationDestination.Credentials,
				ApiKey = replicationDestination.ApiKey,
				Url = replicationDestination.ServerUrl
			};

            string error;
		    string targetServerUrl;

            // since each server can be addresses using both dns and ips we normalize connection string url by fetching target server url
            // it should give us consistent urls
		    if (FetchTargetServerUrl(replicationDestination.ServerUrl, connectionStringOptions, out targetServerUrl, out error) == false)
		    {
                var offlineNode = CountersReplicationTopologyDestinationNode.Offline(replicationDestination.CounterStorageUrl, counterStorage.ServerId);

                if (string.IsNullOrEmpty(error) == false)
                    offlineNode.Errors.Add(error);

                return offlineNode;
		    }

			targetServerUrl = targetServerUrl.ForCounter(replicationDestination.CounterStorageName);

			if (replicationDestination.Disabled)
                return CountersReplicationTopologyDestinationNode.Disabled(targetServerUrl, counterStorage.ServerId);

			if (from.Contains(targetServerUrl))
			{
				var state = CheckConnectionState(replicationDestination.CounterStorageUrl, connectionStringOptions);
				switch (state)
				{
					case ReplicatonNodeState.Online:
                        return CountersReplicationTopologyDestinationNode.Online(targetServerUrl, counterStorage.ServerId);
					case ReplicatonNodeState.Offline:
                        return CountersReplicationTopologyDestinationNode.Offline(targetServerUrl, counterStorage.ServerId);
					default:
						throw new NotSupportedException(state.ToString());
				}
			}

			
			CountersReplicationTopologyRootNode rootNode;
			if (TryGetSchema(targetServerUrl, connectionStringOptions, out rootNode, out error))
			{
                var node = CountersReplicationTopologyDestinationNode.Online(targetServerUrl, counterStorage.ServerId);
				node.Destinations = rootNode.Destinations;
				node.Sources = rootNode.Sources;
				node.Errors = rootNode.Errors;

				return node;
			}

            var offline = CountersReplicationTopologyDestinationNode.Offline(targetServerUrl, counterStorage.ServerId);

			if (string.IsNullOrEmpty(error) == false)
				offline.Errors.Add(error);

			return offline;
		}

	    private bool FetchTargetServerUrl(string serverUrl, RavenConnectionStringOptions connectionStringOptions, out string targetServerUrl, out string error)
	    {
            var url = string.Format("{0}/debug/config", serverUrl);

            try
            {
                var request = requestFactory.Create(url, HttpMethods.Get, connectionStringOptions);
                error = null;
                var ravenConfig = request.ExecuteRequest<RavenJObject>();
                var serverUrlFromTargetConfig = ravenConfig.Value<string>("ServerUrl");

                // replace host name with target hostname
	            targetServerUrl = new UriBuilder(serverUrl) {Host = new Uri(serverUrlFromTargetConfig).Host}.Uri.ToString();
				
                return true;
            }
            catch (Exception e)
            {
                error = e.Message;
                targetServerUrl = null;
                return false;
            }
	    }

	    private bool TryGetSchema(string serverUrl, RavenConnectionStringOptions connectionStringOptions, out CountersReplicationTopologyRootNode rootNode, out string error)
		{
			var url = string.Format("{0}/admin/replication/topology/discover?&ttl={1}", serverUrl, ttl - 1);

			try
			{
				var request = requestFactory.Create(url, HttpMethods.Post, connectionStringOptions);
                request.Write(from);

				error = null;
				rootNode = request.ExecuteRequest<CountersReplicationTopologyRootNode>();

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

        private void FindVisitedNodes(CountersReplicationTopologyNodeBase rootNode, HashSet<string> visitedNodes)
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

		private ReplicatonNodeState CheckConnectionState(string serverUrl, RavenConnectionStringOptions connectionStringOptions)
		{
			try
			{
				var url = string.Format("{0}/replication/heartbeat?&sourceServer={1}", serverUrl, Uri.EscapeDataString(counterStorage.CounterStorageUrl));
				var request = requestFactory.Create(url, HttpMethods.Post, connectionStringOptions);
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