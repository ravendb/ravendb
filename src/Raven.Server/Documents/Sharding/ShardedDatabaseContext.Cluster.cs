using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Sharding.Operations;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public ShardedCluster Cluster;

    public class ShardedCluster
    {
        private readonly ShardedDatabaseContext _context;

        public ShardedCluster([NotNull] ShardedDatabaseContext context)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        /// <summary>
        /// Wait for the database context to be update on all cluster nodes regardless if a shard is actually reside on that node or not [Important when you need the ShardedDatabaseContext to be updated].
        /// </summary>
        public async ValueTask WaitForExecutionOnAllNodesAsync(long index)
        {
            var op = new WaitForIndexNotificationOperation(index);
            await _context.AllNodesExecutor.ExecuteParallelForAllAsync(op);
        }

        /// <summary>
        /// Wait for indexes to be applied on the cluster nodes where the physical database shards are available
        /// </summary>
        public async ValueTask WaitForExecutionOnShardsAsync(long index)
        {
            var op = new WaitForIndexNotificationOperation(index);
            await _context.ShardExecutor.ExecuteParallelForAllAsync(op);
        }

        /// <summary>
        /// Wait for indexes to be applied on the cluster nodes where the physical database shards are available
        /// </summary>
        public async ValueTask WaitForExecutionOnShardsAsync(List<long> indexes)
        {
            var op = new WaitForIndexNotificationOperation(indexes);
            await _context.ShardExecutor.ExecuteParallelForAllAsync(op);
        }
    }
}
