// -----------------------------------------------------------------------
//  <copyright file="ShardedHiLoHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedHiLoHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/hilo/next", "GET")]
        public async Task GetNextHiLo()
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var cmd = await ExecuteShardedHiloCommand(context);
                await cmd.Result.WriteJsonToAsync(ResponseBodyStream());
            }

        }

        [RavenShardedAction("/databases/*/hilo/return", "PUT")]
        public async Task HiLoReturn()
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                await ExecuteShardedHiloCommand(context);
            }
        }

        private async Task<ShardedCommand> ExecuteShardedHiloCommand(TransactionOperationContext context)
        {
            var tag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("tag");
            var hiloDocId = HiLoHandler.RavenHiloIdPrefix + tag;
            var shardIndex = ShardedContext.GetShardIndex(context, hiloDocId);

            var cmd = new ShardedCommand(this, Headers.None);
            await ShardedContext.RequestExecutors[shardIndex].ExecuteAsync(cmd, context);
            
            HttpContext.Response.StatusCode = (int)cmd.StatusCode;
            HttpContext.Response.Headers[Constants.Headers.Etag] = cmd.Response?.Headers?.ETag?.Tag;

            return cmd;
        }
    }
}
