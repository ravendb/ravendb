using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedAdminIndexHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/indexes", "PUT")]
        public async Task Put()
        {
            await PutInternal(validatedAsAdmin: true);
        }

        [RavenShardedAction("/databases/*/indexes", "PUT")]
        public async Task PutJavaScript()
        {
            await PutInternal(validatedAsAdmin: false);
        }

        private async Task PutInternal(bool validatedAsAdmin)
        {
            var isReplicatedQueryString = GetStringQueryString("is-replicated", required: false);
            if (isReplicatedQueryString != null && bool.TryParse(isReplicatedQueryString, out var result) && result)
            {
                // TODO: support legacy index import (replication)
                throw new NotImplementedException();
            }

            
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var input = await context.ReadForMemoryAsync(RequestBodyStream(), "Indexes");

                var exceptions = new List<Exception>();
                for (var shardIndex = 0; shardIndex < ShardedContext.NumberOfShardNodes; shardIndex++)
                {
                    try
                    {
                        var command = new ShardedPutIndexCommand(this, input);
                        var task = ShardedContext.RequestExecutors[shardIndex].ExecuteAsync(command, command.Context);
                        await task;

                        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                        {
                            writer.WritePutIndexResponse(context, command.Result.Results);
                        }
                    }
                    catch (Exception e)
                    {
                        exceptions.Add(e);
                        continue;
                    }

                    // we need only one node to execute the command successfully since this is a cluster command
                    return;
                }

                throw new AggregateException("Failed to put index", exceptions);
            }
        }
    }
}
