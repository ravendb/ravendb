using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.ShardedHandlers.ContinuationTokens;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Threading;

namespace Raven.Server.Documents.Sharding
{
    public partial class ShardedContext
    {
        public ShardedStreaming Streaming;

        public class ShardedStreaming
        {
            private ShardedContext _shardedContext;
            public ShardedStreaming(ShardedContext shardedContext)
            {
                _shardedContext = shardedContext;
            }

            public async IAsyncEnumerable<ShardStreamItem<T>> ShardedStream<T>(CombinedStreamResult combinedStream,
                Func<BlittableJsonReaderObject, ShardStreamItem<T>> converter, Comparer<ShardStreamItem<T>> comparer,
                [EnumeratorCancellation] CancellationToken token)
            {
                await using (var merged = new MergedAsyncEnumerator<ShardStreamItem<T>>(comparer))
                {
                    for (int i = 0; i < combinedStream.Results.Span.Length; i++)
                    {
                        var contextPool = _shardedContext.RequestExecutors[i].ContextPool;
                        var it = new StreamOperation.YieldStreamResults(contextPool, response: combinedStream.Results.Span[i], isQueryStream: false,
                            isTimeSeriesStream: false, isAsync: true, streamQueryStatistics: null);
                        await it.InitializeAsync();
                        await merged.AddAsyncEnumerator(new YieldDocuments<ShardStreamItem<T>>(it, converter, token));
                    }

                    while (await merged.MoveNextAsync(token))
                    {
                        yield return merged.Current;
                    }
                }
            }

            public async IAsyncEnumerable<ShardStreamItem<T>> PagedShardedStream<T>(CombinedStreamResult combinedStream,
                Func<BlittableJsonReaderObject, ShardStreamItem<T>> converter, Comparer<ShardStreamItem<T>> comparer, ShardedPagingContinuation pagingContinuation,
                [EnumeratorCancellation] CancellationToken token)
            {
                var pageSize = pagingContinuation.PageSize;
                using (var context = new ByteStringContext(SharedMultipleUseFlag.None))
                {
                    var results = ShardedStream(combinedStream, converter, comparer, token);
                    await foreach (var result in results.WithCancellation(token))
                    {
                        if (pageSize-- <= 0)
                            yield break;

                        var id = result.Id;
                        var shard =_shardedContext. GetShardIndex(context, id);
                        pagingContinuation.Pages[shard].Start++;

                        yield return result;
                    }
                }
            }

            private class YieldDocuments<T> : AsyncDocumentSession.AbstractYieldStream<T>
            {
                private readonly Func<BlittableJsonReaderObject, T> _converter;

                public YieldDocuments(StreamOperation.YieldStreamResults enumerator, Func<BlittableJsonReaderObject, T> converter, CancellationToken token) : base(enumerator, token)
                {
                    _converter = converter;
                }

                internal override T ResultCreator(StreamOperation.YieldStreamResults asyncEnumerator)
                {
                    return _converter(asyncEnumerator.Current);
                }
            }
        }
    }

    public class ShardedPagingContinuation : ContinuationToken
    {
        public int PageSize;
        public ShardPaging[] Pages;

        public ShardedPagingContinuation()
        {

        }

        public ShardedPagingContinuation(ShardedContext shardedContext, int start, int pageSize)
        {
            var shards = shardedContext.ShardCount;
            var startPortion = start / shards;
            var remaining = start - startPortion * shards;

            Pages = new ShardPaging[shards];

            for (var index = 0; index < Pages.Length; index++)
            {
                Pages[index].Shard = index;
                Pages[index].Start = startPortion;
            }

            Pages[0].Start += remaining;

            PageSize = pageSize;
        }

        public override DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue { [nameof(Pages)] = new DynamicJsonArray(Pages), [nameof(PageSize)] = PageSize };
        }

        public struct ShardPaging : IDynamicJson
        {
            public int Shard;
            public int Start;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue { [nameof(Start)] = Start, [nameof(Shard)] = Shard };
            }
        }
    }

    public class CombinedStreamResult : StreamResult
    {
        public Memory<StreamResult> Results;
    }

    public class ShardStreamItem<T>
    {
        public T Item;
        public LazyStringValue Id;
    }
}
