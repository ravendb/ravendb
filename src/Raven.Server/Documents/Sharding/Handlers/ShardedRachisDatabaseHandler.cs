using System;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedRachisDatabaseHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/rachis/wait-for-raft-commands", "POST")]
        public async Task WaitFor()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var blittable = await context.ReadForMemoryAsync(RequestBodyStream(), "raft-index-ids");
                var commands = JsonDeserializationServer.WaitForRaftCommands(blittable);

                foreach (var index in commands.RaftCommandIndexes)
                {
                    await DatabaseContext.ShardedDatabaseContextIndexNotifications.WaitForIndexNotification(index, HttpContext.RequestAborted);

                    for (int i = 0; i < DatabaseContext.ShardCount; i++)
                    {
                        var name = ShardHelper.ToShardName(DatabaseContext.DatabaseName, i);
                        if (Server.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(name, out var task))
                        {
                            try
                            {
                                var physicalDatabase = await task;
                                await physicalDatabase.RachisLogIndexNotifications.WaitForIndexNotification(index, HttpContext.RequestAborted);
                            }
                            catch (Exception e)
                            {
                                if (Logger.IsInfoEnabled)
                                    Logger.Info($"Failed to wait for an index {index} on shard {i}", e);
                            }
                        }
                    }
                }
            }
        }
    }
}
