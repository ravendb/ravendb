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

        protected ClusterRequestExecutor(X509Certificate2 certificate, DocumentConventions conventions) : base(null, certificate, conventions)
        {
            // Here we are explicitly ignoring trust issues in the case of ClusterRequestExecutor.
            // this is because we don't actually require trust, we just use the certificate
            // as a way to authenticate. Either we encounter the same server certificate which we already  
            // trust, or the admin is going to tell us which specific certs we can trust.
            ServerCertificateCustomValidationCallback += (msg, cert, chain, errors) => true;
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

        public static ClusterRequestExecutor CreateForSingleNode(string url, X509Certificate2 certificate)
        {
            url = ValidateUrls(new[] { url }, certificate)[0];
            var executor = new ClusterRequestExecutor(certificate, DocumentConventions.Default)
            {
                _nodeSelector = new NodeSelector(new Topology
                {
                    Etag = -1,
                    Nodes = new List<ServerNode>
                        {
                            new ServerNode
                            {
                                Url = url
                            }
                        }
                }),
                TopologyEtag = -2,
                _disableTopologyUpdates = true,
                _disableClientConfigurationUpdates = true
            };
            return executor;
        }

        public static ClusterRequestExecutor Create(string[] urls, X509Certificate2 certificate, DocumentConventions conventions = null)
        {
            var executor = new ClusterRequestExecutor(certificate, conventions ?? DocumentConventions.Default)
            {
                _disableClientConfigurationUpdates = true
            };

            executor._firstTopologyUpdate = executor.FirstTopologyUpdate(urls);
            return executor;
        }

        protected override Task PerformHealthCheck(ServerNode serverNode, int nodeIndex, JsonOperationContext context)
        {
            return ExecuteAsync(serverNode, nodeIndex, context, new GetTcpInfoCommand("health-check"), shouldRetry: false);
        }

        public override async Task<bool> UpdateTopologyAsync(ServerNode node, int timeout, bool forceUpdate = false)
        {
            if (_disposed)
                return false;
            var lockTaken = await _clusterTopologySemaphore.WaitAsync(timeout).ConfigureAwait(false);
            if (lockTaken == false)
                return false;
            try
            {
                if (_disposed)
                    return false;

                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new GetClusterTopologyCommand();
                    await ExecuteAsync(node, null, context, command, shouldRetry: false).ConfigureAwait(false);

                    var serverHash = ServerHash.GetServerHash(node.Url);
                    ClusterTopologyLocalCache.TrySavingTopologyToLocalCache(serverHash, command.Result, context);

                    var results = command.Result;
                    var newTopology = new Topology
                    {
                        Nodes = new List<ServerNode>(
                            from member in results.Topology.Members
                            select new ServerNode
                            {
                                Url = member.Value,
                                ClusterTag = member.Key
                            }
                        )
                    };

                    if (_nodeSelector == null)
                    {
                        _nodeSelector = new NodeSelector(newTopology);
                        if (_readBalanceBehavior == ReadBalanceBehavior.FastestNode)
                        {
                            _nodeSelector.ScheduleSpeedTest();
                        }
                    }
                    else if (_nodeSelector.OnUpdateTopology(newTopology, forceUpdate: forceUpdate))
                    {
                        DisposeAllFailedNodesTimers();

                        if (_readBalanceBehavior == ReadBalanceBehavior.FastestNode)
                        {
                            _nodeSelector.ScheduleSpeedTest();
                        }
                    }

                    OnTopologyUpdated(newTopology);
                }
            }
            finally
            {
                _clusterTopologySemaphore.Release();
            }
            return true;
        }

        protected override Task UpdateClientConfigurationAsync()
        {
            return Task.CompletedTask;
        }

        public override void Dispose()
        {
            _clusterTopologySemaphore.Wait();
            base.Dispose();
        }

        protected override bool TryLoadFromCache(string url, JsonOperationContext context)
        {
            var serverHash = ServerHash.GetServerHash(url);
            var cachedTopology = ClusterTopologyLocalCache.TryLoadClusterTopologyFromLocalCache(serverHash, context);

            if (cachedTopology == null)
                return false;

            _nodeSelector = new NodeSelector(new Topology
            {
                Nodes = new List<ServerNode>(
                    from member in cachedTopology.Topology.Members
                    select new ServerNode
                    {
                        Url = member.Value,
                        ClusterTag = member.Key
                    }
                )
            });
            return true;
        }
    }
}
