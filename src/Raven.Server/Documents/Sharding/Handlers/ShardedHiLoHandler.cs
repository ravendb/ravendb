// -----------------------------------------------------------------------
//  <copyright file="ShardedHiLoHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedHiLoHandler : ShardedDatabaseRequestHandler
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
            var shardIndex = DatabaseContext.GetShardIndex(context, hiloDocId);

            var cmd = new ShardedCommand(this, Headers.None);
            await DatabaseContext.RequestExecutors[shardIndex].ExecuteAsync(cmd, context);
            
            HttpContext.Response.StatusCode = (int)cmd.StatusCode;
            HttpContext.Response.Headers[Constants.Headers.Etag] = cmd.Response?.Headers?.ETag?.Tag;

            return cmd;
        }
    }
}
