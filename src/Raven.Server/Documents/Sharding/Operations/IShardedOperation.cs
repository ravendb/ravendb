using System;
using System.Collections.Generic;
using System.Net.Http;
using Microsoft.AspNetCore.Http;
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

        RavenCommand<TResult> CreateCommandForShard(int shard);

        // if the return result is of type blittalbe
        JsonOperationContext CreateOperationContext() => throw new NotImplementedException($"Must be implemented for {typeof(TCombinedResult)}");

        void ModifyHeaders(HttpRequestMessage request)
        {
            foreach (var header in HeadersToCopy)
            {
                if (HttpRequest.Headers.TryGetValue(header, out var value))
                {
                    request.Headers.TryAddWithoutValidation(header, (IEnumerable<string>)value);
                }
            }
        }

        string ModifyUrl(string url) => url;
    }

    public interface IShardedStreamableOperation : IShardedOperation<StreamResult, CombinedStreamResult>
    {
        CombinedStreamResult IShardedOperation<StreamResult, CombinedStreamResult>.Combine(Memory<StreamResult> results) =>
            new CombinedStreamResult { Results = results };
    }
}
