using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Commands;
using Raven.Server.Documents.Sharding.Operations;
using Sparrow.Utils;

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

        public async ValueTask WaitForExecutionOfRaftCommandsAsync(long index)
        {
            await WaitForExecutionOnAllNodes(index);
        }

        public async ValueTask WaitForExecutionOnShards(List<long> indexes)
        {
            var op = new WaitForDatabaseContextUpdateOperation(indexes, useShardedName: true);
            await _context.AllNodesExecutor.ExecuteParallelForAllAsync(op);
        }

        public async Task WaitForExecutionOnAllNodes(long index)
        {
            var op = new WaitForDatabaseContextUpdateOperation(index, useShardedName: false);
            await _context.AllNodesExecutor.ExecuteParallelForAllAsync(op);
        }
    }
}
