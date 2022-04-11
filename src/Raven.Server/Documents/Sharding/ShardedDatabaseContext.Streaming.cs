using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Streaming;
using Raven.Server.Documents.Sharding.Streaming.Comparers;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding
{
    public partial class ShardedDatabaseContext
    {
        public ShardedStreaming Streaming;

        public class ShardedStreaming
        {
            public async ValueTask<Memory<ShardStreamItem<long>>> ReadCombinedLongAsync(
                CombinedReadContinuationState combinedState,
                string name)
            {
                var combined = new ShardStreamItem<long>[combinedState.States.Length];
                for (var index = 0; index < combinedState.States.Length; index++)
                {
                    var state = combinedState.States[index];
                    var property = state.ReadString();
                    if (property != name)
                        state.ThrowInvalidJson();

                    if (await state.ReadAsync() == false)
                        state.ThrowInvalidJson();

                    if (state.CurrentTokenType != JsonParserToken.Integer)
                        state.ThrowInvalidJson();

                    combined[index] = new ShardStreamItem<long>
                    {
                        Item = state.ReadLong,
                        Shard = index
                    };
                }

                return new Memory<ShardStreamItem<long>>(combined);
            }

            public async ValueTask<Memory<ShardStreamItem<T>>> ReadCombinedObjectAsync<T>(
                CombinedReadContinuationState combinedState,
                string name,
                Func<BlittableJsonReaderObject, T> converter)
            {
                var combined = new ShardStreamItem<T>[combinedState.States.Length];
                for (var index = 0; index < combinedState.States.Length; index++)
                {
                    var state = combinedState.States[index];
                    var property = state.ReadString();
                    if (property != name)
                        state.ThrowInvalidJson();

                    var result = await state.ReadObjectAsync();
                    combined[index] = new ShardStreamItem<T>
                    {
                        Item = converter(result),
                        Shard = index
                    };
                }

                return new Memory<ShardStreamItem<T>>(combined);
            }

            public async ValueTask<Memory<ShardStreamItem<T>>> ReadCombinedObjectAsync<T>(
                CombinedReadContinuationState combinedState,
                string name,
                Func<BlittableJsonReaderArray, T> converter)
            {
                var combined = new ShardStreamItem<T>[combinedState.States.Length];
                for (var index = 0; index < combinedState.States.Length; index++)
                {
                    var state = combinedState.States[index];
                    var property = state.ReadString();
                    if (property != name)
                        state.ThrowInvalidJson();

                    var result = await state.ReadJsonArrayAsync();
                    combined[index] = new ShardStreamItem<T>
                    {
                        Item = converter(result),
                        Shard = index
                    };
                }

                return new Memory<ShardStreamItem<T>>(combined);
            }

            public async IAsyncEnumerable<ShardStreamItem<T>> StreamCombinedArray<T>(
                CombinedReadContinuationState combinedState,
                string name,
                Func<BlittableJsonReaderObject, T> converter,
                Comparer<ShardStreamItem<T>> comparer)
            {
                var shards = combinedState.States.Length;
                var iterators = new YieldShardStreamResults[shards];
                for (int i = 0; i < shards; i++)
                {
                    var it = new YieldShardStreamResults(combinedState.States[i], name);
                    await it.InitializeAsync();
                    iterators[i] = it;
                }

                await using (var merged = new MergedAsyncEnumerator<ShardStreamItem<T>>(comparer))
                {
                    for (int i = 0; i < shards; i++)
                    {
                        await merged.AddAsyncEnumerator(new YieldStreamArray<ShardStreamItem<T>, T>(iterators[i], converter, i, combinedState.CancellationToken));
                    }

                    while (await merged.MoveNextAsync(combinedState.CancellationToken))
                    {
                        yield return merged.Current;
                    }
                }
            }

            public async IAsyncEnumerable<ShardStreamItem<T>> PagedShardedStream<T>(
                CombinedReadContinuationState combinedState,
                string name,
                Func<BlittableJsonReaderObject, T> converter,
                Comparer<ShardStreamItem<T>> comparer,
                ShardedPagingContinuation pagingContinuation)
            {
                var pageSize = pagingContinuation.PageSize;
                var results = StreamCombinedArray(combinedState, name, converter, comparer);
                await foreach (var result in results.WithCancellation(combinedState.CancellationToken))
                {
                    if (pageSize-- <= 0)
                        yield break;

                    var shard = result.Shard;
                    pagingContinuation.Pages[shard].Start++;

                    yield return result;
                }
            }

            public IAsyncEnumerable<ShardStreamItem<Document>> PagedShardedDocumentsByLastModified(
                CombinedReadContinuationState combinedState,
                string name,
                ShardedPagingContinuation pagingContinuation)
            {
                return PagedShardedStream(
                    combinedState,
                    name,
                    ShardResultConverter.BlittableToDocumentConverter,
                    StreamDocumentByLastModifiedComparer.Instance,
                    pagingContinuation);
            }

            public async IAsyncEnumerable<Document> GetDocumentsAsync(CombinedReadContinuationState documents, ShardedPagingContinuation pagingContinuation, string resultPropertyName = "Results")
            {
                await foreach (var result in PagedShardedDocumentsByLastModified(documents, resultPropertyName, pagingContinuation))
                {
                    yield return result.Item;
                }
            }

            public IEnumerable<T> PagedShardedItem<T, TInput>(
                Memory<TInput> results,
                Func<TInput, IEnumerable<BlittableJsonReaderObject>> selector,
                Func<BlittableJsonReaderObject, T> converter,
                Comparer<ShardStreamItem<T>> comparer,
                ShardedPagingContinuation pagingContinuation)
            {
                var pageSize = pagingContinuation.PageSize;
                foreach (var result in CombinedResults(results, selector, converter, comparer))
                {
                    if (pageSize-- <= 0)
                        yield break;

                    var shard = result.Shard;
                    pagingContinuation.Pages[shard].Start++;

                    yield return result.Item;
                }
            }

            public IEnumerable<T> PagedShardedItem<T, TInput>(
                Memory<TInput> results,
                Func<TInput, IEnumerable<T>> selector,
                Comparer<ShardStreamItem<T>> comparer,
                ShardedPagingContinuation pagingContinuation)
            {
                var pageSize = pagingContinuation.PageSize;
                foreach (var result in CombinedResults(results, selector, comparer))
                {
                    if (pageSize-- <= 0)
                        yield break;

                    var shard = result.Shard;
                    pagingContinuation.Pages[shard].Start++;

                    yield return result.Item;
                }
            }

            public IEnumerable<ShardStreamItem<T>> CombinedResults<T, TInput>(
                Memory<TInput> results,
                Func<TInput, IEnumerable<T>> selector,
                Comparer<ShardStreamItem<T>> comparer)
            {
                var shards = results.Span.Length;
                using (var merged = new MergedEnumerator<ShardStreamItem<T>>(comparer))
                {
                    for (int i = 0; i < shards; i++)
                    {
                        var shardNumber = i;
                        var r = selector(results.Span[i]);
                        var it = r.Select(item => new ShardStreamItem<T>
                        {
                            Item = item,
                            Shard = shardNumber
                        });
                        merged.AddEnumerator(it.GetEnumerator());
                    }

                    while (merged.MoveNext())
                    {
                        yield return merged.Current;
                    }
                }
            }

            public IEnumerable<ShardStreamItem<T>> CombinedResults<T, TInput>(
                Memory<TInput> results,
                Func<TInput, IEnumerable<BlittableJsonReaderObject>> selector,
                Func<BlittableJsonReaderObject, T> converter,
                Comparer<ShardStreamItem<T>> comparer)
            {
                var shards = results.Span.Length;
                using (var merged = new MergedEnumerator<ShardStreamItem<T>>(comparer))
                {
                    for (int i = 0; i < shards; i++)
                    {
                        var shardNumber = i;
                        var r = selector(results.Span[i]);
                        var it = r.Select(item => new ShardStreamItem<T>
                        {
                            Item = converter(item),
                            Shard = shardNumber
                        });
                        merged.AddEnumerator(it.GetEnumerator());
                    }

                    while (merged.MoveNext())
                    {
                        yield return merged.Current;
                    }
                }
            }

            private class YieldStreamArray<T, TInner> : AsyncDocumentSession.AbstractYieldStream<T> where T : ShardStreamItem<TInner>
            {
                private readonly Func<BlittableJsonReaderObject, TInner> _converter;
                private readonly int _shard;

                public YieldStreamArray(IAsyncEnumerator<BlittableJsonReaderObject> enumerator, Func<BlittableJsonReaderObject, TInner> converter, int shard, CancellationToken token) : base(enumerator, token)
                {
                    _converter = converter;
                    _shard = shard;
                }

                internal override T ResultCreator(IAsyncEnumerator<BlittableJsonReaderObject> asyncEnumerator)
                {
                    return (T)new ShardStreamItem<TInner>
                    {
                        Item = _converter(asyncEnumerator.Current),
                        Shard = _shard
                    };
                }
            }
        }
    }
}
