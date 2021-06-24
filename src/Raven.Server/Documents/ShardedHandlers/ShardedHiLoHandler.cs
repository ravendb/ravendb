// -----------------------------------------------------------------------
//  <copyright file="HiLoHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Identity;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class ShardedHiLoHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/hilo/next", "GET")]
        public async Task GetNextHiLo()
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var tag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("tag");
                var numOfShards = ShardedContext.RequestExecutors.Length;
                var shardIndex = new Random().Next(numOfShards - 1);

                for (int i = 0; i < numOfShards; i++)
                {
                    var cmd = new ShardedCommand(this, Headers.IfMatch);

                    try
                    {
                        await ShardedContext.RequestExecutors[shardIndex].ExecuteAsync(cmd, context);
                    }
                    catch (Exception e)
                    {
                        // failed to reach shard
                        // try the next one
                        shardIndex = (shardIndex + 1) % numOfShards;
                        continue;
                    }

                    HttpContext.Response.StatusCode = (int)cmd.StatusCode;

                    HttpContext.Response.Headers[Constants.Headers.Etag] = cmd.Response?.Headers?.ETag?.Tag;

                    cmd.Result.Modifications = new DynamicJsonValue(cmd.Result)
                    {
                        [nameof(HiLoResult.ShardIndex)] = shardIndex
                    };

                    using (cmd.Result)
                    using (var result = context.ReadObject(cmd.Result, HiLoHandler.RavenHiloIdPrefix + tag, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                        await result.WriteJsonToAsync(ResponseBodyStream());

                    return;
                }
            }
        }
    }
}
