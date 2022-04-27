// -----------------------------------------------------------------------
//  <copyright file="ShardedHiLoHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers.Processors.HiLo;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedHiLoHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/hilo/next", "GET")]
        public async Task GetNextHiLo()
        {
            using (var processor = new ShardedHiLoHandlerProcessorForGetNextHiLo(this))
                await processor.ExecuteAsync();
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
            var shardNumber = DatabaseContext.GetShardNumber(context, hiloDocId);

            var cmd = new ShardedCommand(this, Headers.None);
            await DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(context, cmd, shardNumber);
            
            HttpContext.Response.StatusCode = (int)cmd.StatusCode;
            HttpContext.Response.Headers[Constants.Headers.Etag] = cmd.Response?.Headers?.ETag?.Tag;

            return cmd;
        }
    }
}
