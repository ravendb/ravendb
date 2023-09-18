using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.ServerWide.Commands;
using Sparrow.Json;

namespace Raven.Client.Http
{
    public class ClusterRequestExecutor : RequestExecutor
    {
        private readonly SemaphoreSlim _clusterTopologySemaphore = new SemaphoreSlim(1, 1);

        protected ClusterRequestExecutor(X509Certificate2 certificate, DocumentConventions conventions, string[] initialUrls) : base(null, certificate, conventions, initialUrls)
        {
        }

        [Obsolete("Not supported", error: true)]
        public new static ClusterRequestExecutor Create(string[] urls, string databaseName, X509Certificate2 certificate, DocumentConventions conventions)
        {
            throw new NotSupportedException();
        }

        [Obsolete("Not supported", error: true)]
        public new static ClusterRequestExecutor CreateForSingleNodeWithConfigurationUpdates(string url, string databaseName, X509Certificate2 certificate, DocumentConventions conventions)
        {
            throw new NotSupportedException();
        }

        [Obsolete("Not supported", error: true)]
        public new static ClusterRequestExecutor CreateForSingleNodeWithoutConfigurationUpdates(string url, string databaseName, X509Certificate2 certificate, DocumentConventions conventions)
        {
            throw new NotSupportedException();
        }

        public static ClusterRequestExecutor CreateForSingleNode(string url, X509Certificate2 certificate, DocumentConventions conventions = null)
        {
            var initialUrls = new[] { url };
            var urls = ValidateUrls(initialUrls, certificate);
            var executor = new ClusterRequestExecutor(certificate, conventions ?? DocumentConventions.Default, initialUrls)
            {
                _nodeSelector = new NodeSelector(new Topology
                {
                    Etag = -1,
                    Nodes = new List<ServerNode>
                    {
                        new ServerNode
                        {
                            Url = urls[0],
                            ServerRole = ServerNode.Role.Member
                        }
                    }
                }),
                TopologyEtag = -2,
                _disableTopologyUpdates = true,
                _disableClientConfigurationUpdates = true,
                _topologyHeaderName = Constants.Headers.ClusterTopologyEtag
            };
            // This is just to fetch the cluster tag
            executor._firstTopologyUpdate = executor.SingleTopologyUpdateAsync(urls, null);
            return executor;
        }

        internal static ClusterRequestExecutor CreateForShortTermUse(string url, X509Certificate2 certificate, DocumentConventions conventions = null)
        {
            var initialUrls = new[] { url };
            var urls = ValidateUrls(initialUrls, certificate);
            var executor = new ClusterRequestExecutor(certificate, conventions ?? DocumentConventions.Default, initialUrls)
            {
                _nodeSelector = new NodeSelector(new Topology
                {
                    Etag = -1,
                    Nodes = new List<ServerNode>
                    {
                        new ServerNode
                        {
                            Url = urls[0],
                            ServerRole = ServerNode.Role.Member
                        }
                    }
                }),
                TopologyEtag = -2,
                _disableTopologyUpdates = true,
                _disableClientConfigurationUpdates = true,
                _topologyHeaderName = Constants.Headers.ClusterTopologyEtag
            };
            // Not fetching the node tag because this executor could be used during the cluster creation still
            executor._firstTopologyUpdate = Task.CompletedTask;
            return executor;
        }

        public static ClusterRequestExecutor Create(string[] initialUrls, X509Certificate2 certificate, DocumentConventions conventions = null)
        {
            var executor = new ClusterRequestExecutor(certificate, conventions ?? DocumentConventions.Default, initialUrls)
            {
                _disableClientConfigurationUpdates = true,
                _topologyHeaderName = Constants.Headers.ClusterTopologyEtag
            };

            executor._firstTopologyUpdate = executor.FirstTopologyUpdate(initialUrls, null);
            return executor;
        }

        protected override Task PerformHealthCheck(ServerNode serverNode, int nodeIndex, JsonOperationContext context)
        {
            return ExecuteAsync(serverNode, nodeIndex, context, new GetTcpInfoCommand("health-check"), shouldRetry: false, sessionInfo: null, token: CancellationToken.None);
        }

        public override async Task<bool> UpdateTopologyAsync(UpdateTopologyParameters parameters)
        {
            if (parameters is null)
                throw new ArgumentNullException(nameof(parameters));

            if (Disposed)
                return false;
            var lockTaken = await _clusterTopologySemaphore.WaitAsync(parameters.TimeoutInMs).ConfigureAwait(false);
            if (lockTaken == false)
                return false;
            try
            {
                if (Disposed)
                    return false;

                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new GetClusterTopologyCommand(parameters.DebugTag);
                    await ExecuteAsync(parameters.Node, null, context, command, shouldRetry: false, sessionInfo: null, token: CancellationToken.None).ConfigureAwait(false);

                    await ClusterTopologyLocalCache.TrySavingAsync(TopologyHash, command.Result, Conventions, context, CancellationToken.None).ConfigureAwait(false);

                    var results = command.Result;
                    var nodes = ServerNode.CreateFrom(results.Topology);

                    var newTopology = new Topology
                    {
                        Nodes = nodes,
                        Etag = results.Etag
                    };
                    
                    UpdateNodeSelector(newTopology, parameters.ForceUpdate);

                    OnTopologyUpdatedInvoke(newTopology);
                }
            }
            catch (Exception)
            {
                if (Disposed == false)
                    throw;
            }
            finally
            {
                _clusterTopologySemaphore.Release();
            }

            return true;
        }

        protected override Task UpdateClientConfigurationAsync(ServerNode serverNode)
        {
            return Task.CompletedTask;
        }

        protected override void ThrowExceptions(List<(string, Exception)> list)
        {
            throw new AggregateException("Failed to retrieve cluster topology from all known nodes" + Environment.NewLine +
                                         string.Join(Environment.NewLine, list.Select(x => x.Item1 + " -> " + x.Item2?.Message))
                , list.Select(x => x.Item2));
        }

        protected override async Task<bool> TryLoadFromCacheAsync(JsonOperationContext context)
        {
            var clusterTopology = await ClusterTopologyLocalCache.TryLoadAsync(TopologyHash, Conventions, context).ConfigureAwait(false);
            if (clusterTopology == null)
                return false;

            var nodes = ServerNode.CreateFrom(clusterTopology.Topology);

            _nodeSelector = new NodeSelector(new Topology
            {
                Nodes = nodes
            });

            return true;
        }
    }
}
