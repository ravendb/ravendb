using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Commands;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public partial class ShardedDatabaseRequestHandler
    {
        public class ShardedClusterHandler
        {
            private readonly ShardedDatabaseRequestHandler _handler;

            public ShardedClusterHandler(ShardedDatabaseRequestHandler handler)
            {
                _handler = handler;
            }

            public async Task WaitForExecutionOfRaftCommandsAsync(JsonOperationContext context, List<long> raftIndexIds)
            {
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Should be modified after we migrate to ShardedExecutor");

                using (var cts = CancellationTokenSource.CreateLinkedTokenSource(_handler.ServerStore.ServerShutdown))
                {
                    var timeToWait = _handler.ServerStore.Configuration.Cluster.OperationTimeout.GetValue(TimeUnit.Milliseconds) * raftIndexIds.Count;
                    cts.CancelAfter(TimeSpan.FromMilliseconds(timeToWait));

                    var requestExecutors = _handler.DatabaseContext.RequestExecutors;
                    var waitingTasks = new Task[requestExecutors.Length];

                    var waitForDatabaseCommands = new WaitForRaftCommands(raftIndexIds);
                    for (var index = 0; index < requestExecutors.Length; index++)
                    {
                        waitingTasks[index] = requestExecutors[index].ExecuteAsync(waitForDatabaseCommands, context, token: cts.Token);
                    }
                    await Task.WhenAll(waitingTasks);
                }
            }
        }
    }
}
