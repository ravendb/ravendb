using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Server.Commands;
using Sparrow.Json;

namespace Raven.Client.Http
{
    public class ClusterRequestExecutor : RequestExecutor
    {
        private readonly SemaphoreSlim _clusterTopologySemaphore = new SemaphoreSlim(1, 1);

        protected ClusterRequestExecutor(string apiKey, bool isSingleNode) : base(null, apiKey,isSingleNode)
        {
        }

        [Obsolete("Not supported", error: true)]
        public new static ClusterRequestExecutor Create(string[] urls, string databaseName, string apiKey, ClusterMode clusterMode = ClusterMode.Failover)
        {
            throw new NotSupportedException();
        }

        [Obsolete("Not supported", error: true)]
        public new static ClusterRequestExecutor CreateForSingleNode(string url, string databaseName, string apiKey)
        {
            throw new NotSupportedException();
        }

        public static ClusterRequestExecutor CreateForSingleNode(string url, string apiKey)
        {
            var executor = new ClusterRequestExecutor(apiKey,true)
            {
                _nodeSelector = new FailoverNodeSelector(new Topology
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

        public static ClusterRequestExecutor Create(string[] urls, string apiKey)
        {
            var executor = new ClusterRequestExecutor(apiKey,false)
            {
                _disableClientConfigurationUpdates = true
            };

            executor._firstTopologyUpdate = 
                executor.FirstTopologyUpdate(urls)
                        .ContinueWith(_ =>
                        {
                            executor._nodeSelector = executor.GetNodeSelector(new Topology
                            {
                                Nodes = executor.TopologyNodes?.ToList() ?? new List<ServerNode>(),
                                Etag = executor.TopologyEtag
                            });
                        });
            return executor;
        }

        protected override Task PerformHealthCheck(ServerNode serverNode, JsonOperationContext context)
        {
            return ExecuteAsync(serverNode, context, new GetTcpInfoCommand("health-check"), shouldRetry: false);
        }

        public override async Task<bool> UpdateTopologyAsync(ServerNode node, int timeout)
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

                    await ExecuteAsync(node, context, command, shouldRetry: false).ConfigureAwait(false);

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
                        _nodeSelector = new FailoverNodeSelector(newTopology);
                    }
                    else if (_nodeSelector.OnUpdateTopology(newTopology))
                    {
                        DisposeAllFailedNodesTimers();
                    }

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

            _nodeSelector = new FailoverNodeSelector(new Topology
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