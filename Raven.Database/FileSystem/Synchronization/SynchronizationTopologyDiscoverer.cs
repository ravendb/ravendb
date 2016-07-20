using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Database.Bundles.Replication.Data;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Synchronization
{
    internal class SynchronizationTopologyDiscoverer
    {
        private readonly RavenFileSystem filesystem;

        private readonly int ttl;

        private readonly ILog log;

        private readonly RavenJArray @from;

        private readonly HttpRavenRequestFactory requestFactory;

        private readonly Guid currentServerId;

        public SynchronizationTopologyDiscoverer(RavenFileSystem filesystem, RavenJArray @from, int ttl, ILog log)
        {
            this.filesystem = filesystem;
            this.ttl = ttl;
            this.log = log;
            this.@from = @from;
            requestFactory = new HttpRavenRequestFactory();

            currentServerId = filesystem.Storage.Id;
        }

        public SynchronizationTopologyRootNode Discover()
        {
            var root = new SynchronizationTopologyRootNode(filesystem.SynchronizationTask.FileSystemUrl, filesystem.Storage.Id);

            if (ttl <= 0)
                return root;

            var syncDestinations = filesystem.SynchronizationTask.GetSynchronizationDestinations().ToList();

            IList<string> sourceNames = null;
            filesystem.Storage.Batch(accessor =>
            {
                int totalResults;
                sourceNames = accessor.GetConfigNamesStartingWithPrefix(SynchronizationConstants.RavenSynchronizationSourcesBasePath, 0, int.MaxValue,
                                                                      out totalResults);
            });

            if (@from.Contains(filesystem.SynchronizationTask.FileSystemUrl) == false)
            {
                @from.Add(filesystem.SynchronizationTask.FileSystemUrl);
            }

            root.Destinations = HandleDestinations(syncDestinations);
            root.Sources = HandleSources(sourceNames, root);

            return root;
        }

        private List<SynchronizationTopologySourceNode> HandleSources(IEnumerable<string> sources, SynchronizationTopologyRootNode root)
        {
            var nodes = new List<SynchronizationTopologySourceNode>();
            foreach (var source in sources)
            {
                RavenJObject sourceAsJson = null;

                filesystem.Storage.Batch(accessor =>
                {
                    sourceAsJson = accessor.GetConfig(source);
                });

                SourceSynchronizationInformation sourceInfo = null;
                try
                {
                    sourceInfo = sourceAsJson.JsonDeserialization<SourceSynchronizationInformation>();
                }
                catch (Exception)
                {
                    root.Errors.Add("Could not deserialize source node.");
                }

                var sourceDatabaseId = Guid.Parse(source.Split('/').Last());
                var node = HandleSource(sourceInfo, sourceDatabaseId);
                nodes.Add(node);
            }

            return nodes;
        }

        private SynchronizationTopologySourceNode HandleSource(
            SourceSynchronizationInformation source, Guid sourceDatabaseId)
        {
            if (from.Contains(source.SourceServerUrl))
            {
                var state = CheckSourceConnectionState(source.SourceServerUrl);
                switch (state)
                {
                    case ReplicatonNodeState.Online:
                        return SynchronizationTopologySourceNode.Online(source.SourceServerUrl,
                            sourceDatabaseId, currentServerId, source.LastSourceFileEtag);
                    case ReplicatonNodeState.Offline:
                        return SynchronizationTopologySourceNode.Offline(source.SourceServerUrl,
                            sourceDatabaseId, currentServerId, source.LastSourceFileEtag);
                    default:
                        throw new NotSupportedException(state.ToString());
                }
            }

            string error;
            SynchronizationTopologyRootNode rootNode;
            if (TryGetSchema(source.SourceServerUrl, new RavenConnectionStringOptions(), out rootNode, out error))
            {
                var node = SynchronizationTopologySourceNode.Online(source.SourceServerUrl,
                    sourceDatabaseId, currentServerId, source.LastSourceFileEtag);
                node.Destinations = rootNode.Destinations;
                node.Sources = rootNode.Sources;
                node.Errors = rootNode.Errors;

                return node;
            }

            var offline = SynchronizationTopologySourceNode.Offline(source.SourceServerUrl,
                sourceDatabaseId, currentServerId, source.LastSourceFileEtag);

            if (string.IsNullOrEmpty(error) == false)
                offline.Errors.Add(error);

            return offline;
        }

        private List<SynchronizationTopologyDestinationNode> HandleDestinations(IList<SynchronizationDestination> destinations)
        {
            return destinations
                .Select(HandleDestination)
                .ToList();
        }

        private SynchronizationTopologyDestinationNode HandleDestination(SynchronizationDestination synchronizationDestination)
        {
            var connectionStringOptions = new RavenConnectionStringOptions
            {
                Credentials = synchronizationDestination.Credentials,
                ApiKey = synchronizationDestination.ApiKey,
                Url = synchronizationDestination.ServerUrl,
                DefaultDatabase = synchronizationDestination.FileSystem
            };

            string error;
            string targetServerUrl;
            var serverUrl = synchronizationDestination.Url;
            // since each server can be addresses using both dns and ips we normalize connection string url by fetching target server url
            // it should give us consistent urls
            Guid? destinationFileSystemId;
            if (FetchTargetServerUrl(serverUrl, connectionStringOptions, 
                out targetServerUrl, out destinationFileSystemId, out error) == false)
            {
                var offlineNode = SynchronizationTopologyDestinationNode.Offline(serverUrl, currentServerId, destinationFileSystemId);

                if (string.IsNullOrEmpty(error) == false)
                    offlineNode.Errors.Add(error);

                return offlineNode;
            }

            if (synchronizationDestination.Enabled == false)
                return SynchronizationTopologyDestinationNode.Disabled(targetServerUrl, currentServerId, destinationFileSystemId);

            if (from.Contains(targetServerUrl))
            {
                var state = CheckConnectionState(synchronizationDestination.Url, connectionStringOptions);
                switch (state)
                {
                    case ReplicatonNodeState.Online:
                        return SynchronizationTopologyDestinationNode.Online(targetServerUrl, currentServerId, destinationFileSystemId);
                    case ReplicatonNodeState.Offline:
                        return SynchronizationTopologyDestinationNode.Offline(targetServerUrl, currentServerId, destinationFileSystemId);
                    default:
                        throw new NotSupportedException(state.ToString());
                }
            }

            SynchronizationTopologyRootNode rootNode;
            if (TryGetSchema(targetServerUrl, connectionStringOptions, out rootNode, out error))
            {
                var node = SynchronizationTopologyDestinationNode.Online(targetServerUrl, currentServerId, destinationFileSystemId);
                node.Destinations = rootNode.Destinations;
                node.Sources = rootNode.Sources;
                node.Errors = rootNode.Errors;

                return node;
            }

            var offline = SynchronizationTopologyDestinationNode.Offline(targetServerUrl, currentServerId, destinationFileSystemId);

            if (string.IsNullOrEmpty(error) == false)
                offline.Errors.Add(error);

            return offline;
        }

        private bool FetchTargetServerUrl(string serverUrl, RavenConnectionStringOptions connectionStringOptions, 
            out string targetServerUrl, out Guid? fileSystemId, out string error)
        {
            try
            {
                var url = $"{serverUrl}/stats";

                var request = requestFactory.Create(url, HttpMethods.Get, connectionStringOptions);
                error = null;
                var ravenConfig = request.ExecuteRequest<RavenJObject>();
                var serverUrlFromTargetConfig = ravenConfig.Value<string>("ServerUrl");
                var databaseIdString = ravenConfig.Value<string>("FileSystemId");

                fileSystemId = null;
                Guid localDatabaseId;
                if (Guid.TryParse(databaseIdString, out localDatabaseId))
                    fileSystemId = localDatabaseId;

                // replace host name with target hostname
                targetServerUrl = new UriBuilder(serverUrl) { Host = new Uri(serverUrlFromTargetConfig).Host }.Uri.ToString();
                
                return true;
            }
            catch (Exception e)
            {
                error = e.Message;
                targetServerUrl = null;
                fileSystemId = null;
                return false;
            }
        }

        private Guid? GetFileSystemId(string serverUrl, RavenConnectionStringOptions connectionStringOptions)
        {
            try
            {
                var fsUrl = serverUrl.ForFilesystem(connectionStringOptions.DefaultDatabase);
                var url = $"{fsUrl}/stats";
                var request = requestFactory.Create(url, HttpMethods.Get, connectionStringOptions);
                var ravenConfig = request.ExecuteRequest<RavenJObject>();
                var databaseIdString = ravenConfig.Value<string>("FileSystemId");

                Guid localDatabaseId;
                if (Guid.TryParse(databaseIdString, out localDatabaseId) == false)
                    return null;

                return localDatabaseId;
            }
            catch (Exception e)
            {
                return null;
            }
        }

        private bool TryGetSchema(string serverUrl, RavenConnectionStringOptions connectionStringOptions, out SynchronizationTopologyRootNode rootNode, out string error)
        {
            var url = $"{serverUrl}/admin/replication/topology/discover?&ttl={ttl - 1}";

            try
            {
                var request = requestFactory.Create(url, HttpMethods.Post, connectionStringOptions);
                request.Write(from);

                error = null;
                rootNode = request.ExecuteRequest<SynchronizationTopologyRootNode>();

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

        private void FindVisitedNodes(SynchronizationTopologyNodeBase rootNode, HashSet<string> visitedNodes)
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
                var url = $"{serverUrl}/stats";
                var request = requestFactory.Create(url, HttpMethods.Get, connectionStringOptions);
                request.ExecuteRequest();
            }
            catch (Exception e)
            {
                log.ErrorException($"Could not connect to '{serverUrl}'.", e);

                return ReplicatonNodeState.Offline;
            }

            return ReplicatonNodeState.Online;
        }
    }
}
