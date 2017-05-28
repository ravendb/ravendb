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

        protected ClusterRequestExecutor(string apiKey) : base(null, apiKey)
        {
        }

        [Obsolete("Not supported", error: true)]
        public new static ClusterRequestExecutor Create(string[] urls, string databaseName, string apiKey)
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
            {
                var executor = new ClusterRequestExecutor(apiKey)
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
                    _withoutTopology = true
                };
                return executor;
            }
        }

        public static ClusterRequestExecutor Create(string[] urls, string apiKey)
        {
            var executor = new ClusterRequestExecutor(apiKey);
            executor._firstTopologyUpdate = executor.FirstTopologyUpdate(urls);
            return executor;
        }

        protected override async Task FirstTopologyUpdate(string[] initialUrls)
        {
            var list = new List<Exception>();
            foreach (var url in initialUrls)
            {
                try
                {
                    await GetClusterTopologyAsync(new ServerNode
                    {
                        Url = url,
                        Database = _databaseName
                    }, Timeout.Infinite)
                        .ConfigureAwait(false);
                    return;
                }
                catch (Exception e)
                {
                    list.Add(new Exception($"{url} - {e.Message}", e));
                }
            }

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                foreach (var url in initialUrls)
                {
                    if (TryLoadFromCache(url, context) == false)
                        continue;

                    return;
                }
            }

            _lastKnownUrls = initialUrls;

            throw new AggregateException("Failed to retrieve cluster topology from all known nodes", list);
        }

        public async Task<bool> GetClusterTopologyAsync(ServerNode node, int timeout)
        {
            if (_disposed)
                return false;
            var lockTaken = _clusterTopologySemaphore.Wait(timeout);
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
                    _nodeSelector = new NodeSelector(new Topology
                    {
                        Nodes = new List<ServerNode>
                        {
                            new ServerNode
                            {
                                Url = results.Topology.Members.First().Value,
                                ClusterTag = results.Topology.Members.First().Key
                            }
                        }
                    });
                }
            }
            finally
            {
                _clusterTopologySemaphore.Release();
            }
            return true;
        }

        private bool TryLoadFromCache(string url, JsonOperationContext context)
        {
            var serverHash = ServerHash.GetServerHash(url);
            var cachedTopology = ClusterTopologyLocalCache.TryLoadClusterTopologyFromLocalCache(serverHash, context);

            if (cachedTopology == null)
                return false;

            _nodeSelector = new NodeSelector(new Topology
            {
                Nodes = new List<ServerNode>
                {
                    new ServerNode
                    {
                        Url = cachedTopology.Topology.Members.First().Value,
                        ClusterTag = cachedTopology.Topology.Members.First().Key
                    }
                }
            });
            return true;
        }

    }
}