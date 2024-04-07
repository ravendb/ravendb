using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using NuGet.Packaging;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Commands.Revisions;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers.Processors.Revisions;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors;

internal class ShardedAdminForbiddenUnusedIdsHandlerProcessorForGetUnusedIds : AbstractAdminForbiddenUnusedIdsHandlerProcessorForGetUnusedIds<
    ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedAdminForbiddenUnusedIdsHandlerProcessorForGetUnusedIds([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async Task ValidateUnusedIdsOnAllNodesAsync(HashSet<string> unusedIds, Dictionary<string, string> forbiddenIds, string databaseName,
        CancellationToken token)
    {
        var op = new ShardedGetForbiddenUnusedIdsOperation(RequestHandler,
            new ShardedGetForbiddenUnusedIdsOperation.Parameters()
            {
                ValidateContent = false, // We want to validate the ids content only once - only on the orchestrator, not on every shard
                DatabaseIds = unusedIds
            });
        var results = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op, token);

        forbiddenIds.AddRange(results);
    }

    internal readonly struct ShardedGetForbiddenUnusedIdsOperation : IShardedOperation<Dictionary<string, string>, Dictionary<string, string>>
    {
        private readonly ShardedDatabaseRequestHandler _handler;
        private readonly Parameters _parameters;

        public ShardedGetForbiddenUnusedIdsOperation(ShardedDatabaseRequestHandler handler, Parameters parameters)
        {
            _handler = handler;
            _parameters = parameters;
        }

        public HttpRequest HttpRequest => _handler.HttpContext.Request;

        public Dictionary<string, string> Combine(Dictionary<int, ShardExecutionResult<Dictionary<string, string>>> results)
        {
            var combined = new Dictionary<string, string>();

            foreach (var (shardNumber, dict) in results)
            {
                combined.AddRangeIgnoringExisted(dict.Result);
            }

            return combined;
        }

        public RavenCommand<Dictionary<string, string>> CreateCommandForShard(int shardNumber) => new GetForbiddenUnusedIdsCommand(_parameters);


        internal class GetForbiddenUnusedIdsCommand : RavenCommand<Dictionary<string, string>>
        {
            public override bool IsReadRequest { get; }
            private readonly Parameters _parameters;

            internal GetForbiddenUnusedIdsCommand(Parameters parameters)
            {
                _parameters = parameters;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases/{node.Database}/get-forbidden-unused-ids";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                    Content = new BlittableJsonContent(
                        async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_parameters, ctx))
                            .ConfigureAwait(false), DocumentConventions.Default)
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                {
                    Result = null;
                    return;
                }

                var dict = new Dictionary<string, string>();
                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                for (var i = 0; i < response.Count; i++)
                {
                    response.GetPropertyByIndex(i, ref propertyDetails);
                    dict[propertyDetails.Name] = propertyDetails.Value.ToString();
                }

                Result = dict;
            }
        }


        internal sealed class Parameters
        {
            public HashSet<string> DatabaseIds { get; set; }
            public bool ValidateContent { get; set; }
        }
    }
}

public static class DictionaryExtensions
{
    public static void AddRangeIgnoringExisted<E, T>(this Dictionary<E, T> dictionary, IDictionary<E, T> items)
    {
        foreach (var kvp in items)
        {
            if (dictionary.ContainsKey(kvp.Key) == false)
                dictionary.Add(kvp.Key, kvp.Value);
        }
    }
}
