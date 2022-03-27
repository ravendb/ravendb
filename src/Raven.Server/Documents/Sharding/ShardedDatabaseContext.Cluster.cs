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
            await WaitForAllNodesToUpdateDatabaseContext(index);
        }

        public async ValueTask WaitForExecutionOfRaftCommandsAsync(List<long> indexes)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Should be modified after we migrate to ShardedExecutor");

            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(_context.DatabaseShutdown))
            {
                var timeToWait = _context.Configuration.Cluster.OperationTimeout.GetValue(TimeUnit.Milliseconds) * indexes.Count;
                cts.CancelAfter(TimeSpan.FromMilliseconds(timeToWait));

                var requestExecutors = _context.RequestExecutors;
                var waitingTasks = new Task[requestExecutors.Length];

                var waitForDatabaseCommands = new WaitForRaftCommands(indexes);
                for (var index = 0; index < _context.FullRange.Length; index++)
                {
                    var shardNumber = _context.FullRange[index];
                    waitingTasks[index] = _context.ShardExecutor.ExecuteSingleShardAsync(waitForDatabaseCommands, shardNumber, cts.Token);
                }

                await Task.WhenAll(waitingTasks);
            }
        }

        public async Task WaitForAllNodesToUpdateDatabaseContext(long index)
        {
            var op = new WaitForDatabaseContextUpdateOperation(_context.DatabaseName, index);
            await _context.ShardExecutor.ExecuteParallelForShardsAsync(_context.UniqueShards, op);
        }
    }
}
