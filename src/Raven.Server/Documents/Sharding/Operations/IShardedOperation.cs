using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Reflection.Metadata;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Streaming;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations
{
    public interface IShardedOperation<TResult> : IShardedOperation<TResult, TResult>
    {
    }

    public interface IShardedOperation : IShardedOperation<object>
    {
        object IShardedOperation<object, object>.Combine(Memory<object> results) => null;
    }

    public interface IShardedOperation<TResult, out TCombinedResult>
    {
        HttpRequest HttpRequest { get; }

        TCombinedResult Combine(Memory<TResult> results);

        List<string> HeadersToCopy => ShardedDatabaseRequestHandler.HeadersToCopy;

        TCombinedResult CombineCommands(Memory<RavenCommand<TResult>> commands, Memory<TResult> results)
        {
            var span = commands.Span;
            for (int i = 0; i < span.Length; i++)
            {
                results.Span[i] = span[i].Result;
            }

            return Combine(results);
        }

        RavenCommand<TResult> CreateCommandForShard(int shardNumber);

        // if the return result is of type blittalbe
        JsonOperationContext CreateOperationContext() => throw new NotImplementedException($"Must be implemented for {typeof(TCombinedResult)}");

        void ModifyHeaders(HttpRequestMessage request)
        {
            if (HttpRequest == null)
                return; // this will happen if we are scheduling sharded operation not from a processor, e.g. ShardedIndexCreateController

            foreach (var header in HeadersToCopy)
            {
                if (request.Headers.Contains(header))
                    continue;

                if (HttpRequest.Headers.TryGetValue(header, out var value))
                {
                    request.Headers.TryAddWithoutValidation(header, (IEnumerable<string>)value);
                }
            }

            request.Headers.TryAddWithoutValidation(Constants.Headers.Sharded, "true");
        }

        string ModifyUrl(string url) => url;
    }

    public interface IShardedStreamableOperation : IShardedReadOperation<StreamResult, CombinedStreamResult>
    {
        ShardedReadResult<CombinedStreamResult> IShardedOperation<StreamResult, ShardedReadResult<CombinedStreamResult>>.Combine(Memory<StreamResult> results) =>
            new() {Result = new CombinedStreamResult {Results = results}};
    }
}
