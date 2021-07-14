// -----------------------------------------------------------------------
//  <copyright file="ShardedHiLoHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Identity;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ShardedHandlers
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
                var shardIndex = ShardedContext.GetShardIndex(context, DateTime.UtcNow.Ticks.ToString());

                for (int i = 0; i < numOfShards; i++)
                {
                    var cmd = new ShardedCommand(this, Headers.IfMatch);

                    try
                    {
                        await ShardedContext.RequestExecutors[shardIndex].ExecuteAsync(cmd, context);
                    }
                    catch (Exception)
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
                    {
                        await result.WriteJsonToAsync(ResponseBodyStream());
                    }

                    return;
                }
            }
        }

        [RavenShardedAction("/databases/*/hilo/return", "PUT")]
        public async Task HiLoReturn()
        {
            var shardIndex = GetLongQueryString("shardIndex", required: true);
            Debug.Assert(shardIndex.HasValue);
            if (shardIndex >= ShardedContext.RequestExecutors.Length)
                throw new ArgumentException($"Value of query string 'shardIndex' is invalid, got '{shardIndex}' while the number of shards is '{ShardedContext.RequestExecutors.Length}'.");

            var cmd = new ShardedCommand(this, Headers.IfMatch);
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                // ReSharper disable once PossibleInvalidOperationException
                await ShardedContext.RequestExecutors[shardIndex.Value].ExecuteAsync(cmd, context);
            }

            HttpContext.Response.StatusCode = (int)cmd.StatusCode;
            HttpContext.Response.Headers[Constants.Headers.Etag] = cmd.Response?.Headers?.ETag?.Tag;
        }
    }
}
