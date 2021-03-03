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
            url = ValidateUrls(initialUrls, certificate)[0];
            var executor = new ClusterRequestExecutor(certificate, conventions ?? DocumentConventions.Default, initialUrls)
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

        public static ClusterRequestExecutor Create(string[] initialUrls, X509Certificate2 certificate, DocumentConventions conventions = null)
        {
            var executor = new ClusterRequestExecutor(certificate, conventions ?? DocumentConventions.Default, initialUrls)
            {
                _disableClientConfigurationUpdates = true
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
                    var newTopology = new Topology
                    {
                        Nodes = new List<ServerNode>(
                            from member in results.Topology.Members
                            select new ServerNode
                            {
                                Url = member.Value,
                                ClusterTag = member.Key
                            }
                        ),
                        Etag = results.Etag
                    };

                    TopologyEtag = results.Etag;

                    if (_nodeSelector == null)
                    {
                        _nodeSelector = new NodeSelector(newTopology);
                        if (Conventions.ReadBalanceBehavior == ReadBalanceBehavior.FastestNode)
                        {
                            _nodeSelector.ScheduleSpeedTest();
                        }
                    }
                    else if (_nodeSelector.OnUpdateTopology(newTopology, forceUpdate: parameters.ForceUpdate))
                    {
                        DisposeAllFailedNodesTimers();

                        if (Conventions.ReadBalanceBehavior == ReadBalanceBehavior.FastestNode)
                        {
                            _nodeSelector.ScheduleSpeedTest();
                        }
                    }

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

            _nodeSelector = new NodeSelector(new Topology
            {
                Nodes = new List<ServerNode>(
                    from member in clusterTopology.Topology.Members
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
