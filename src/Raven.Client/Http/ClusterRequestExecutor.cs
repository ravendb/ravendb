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

        protected ClusterRequestExecutor(string databaseName, string apiKey) : base(databaseName, apiKey)
        {
        }

        public new static ClusterRequestExecutor Create(string[] urls, string databaseName, string apiKey)
        {
            var executor = new ClusterRequestExecutor(databaseName, apiKey);
            executor._firstTopologyUpdate = executor.FirstTopologyUpdate(urls);
            return executor;
        }

        public new static RequestExecutor CreateForSingleNode(string url, string databaseName, string apiKey)
        {
            return Create(new[] { url }, databaseName, apiKey);
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
                    list.Add(e);
                }
            }

            //TODO: cache for cluster topology

            _lastKnownUrls = initialUrls;

            throw new AggregateException("Failed to retrieve clsuter topology from all known nodes", list);
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

    }
}