using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Streaming;
using Raven.Server.Documents.Sharding.Streaming.Comparers;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding
{
    public partial class ShardedDatabaseContext
    {
        public ShardedStreaming Streaming;

        public sealed class ShardedStreaming
        {
            public async ValueTask<Memory<ShardStreamItem<long>>> ReadCombinedLongAsync(
                CombinedReadContinuationState combinedState,
                string name)
            {
                var combined = new ShardStreamItem<long>[combinedState.States.Count];
                int index = 0;
                foreach (var shardNumber in combinedState.States.Keys)
                {
                    var state = combinedState.States[shardNumber];
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
                        ShardNumber = shardNumber
                    };
                    index++;
                }

                return new Memory<ShardStreamItem<long>>(combined);
            }

            public async ValueTask<Memory<ShardStreamItem<T>>> ReadCombinedObjectAsync<T>(
                CombinedReadContinuationState combinedState,
                string name,
                Func<BlittableJsonReaderArray, T> converter)
            {
                var combined = new ShardStreamItem<T>[combinedState.States.Count];
                int index = 0;
                foreach (var shardNumber in combinedState.States.Keys)
                {
                    var state = combinedState.States[shardNumber];
                    var property = state.ReadString();
                    if (property != name)
                        state.ThrowInvalidJson();

                    var result = await state.ReadJsonArrayAsync();
                    combined[index] = new ShardStreamItem<T>
                    {
                        Item = converter(result),
                        ShardNumber = shardNumber
                    };
                    index++;
                }

                return new Memory<ShardStreamItem<T>>(combined);
            }

            public async IAsyncEnumerable<ShardStreamItem<T>> StreamCombinedArray<T>(
                CombinedReadContinuationState combinedState,
                string name,
                Func<BlittableJsonReaderObject, T> converter,
                Comparer<ShardStreamItem<T>> comparer)
            {
                var shards = combinedState.States.Count;
                var iterators = new Dictionary<int, YieldShardStreamResults>(shards);
                foreach (var shardToState in combinedState.States)
                {
                    var it = new YieldShardStreamResults(shardToState.Value, name);
                    await it.InitializeAsync();
                    iterators[shardToState.Key] = it;
                }

                await using (var merged = new MergedAsyncEnumerator<ShardStreamItem<T>>(comparer))
                {
                    foreach (var shardToIterator in iterators)
                    {
                        await merged.AddAsyncEnumerator(new YieldStreamArray<ShardStreamItem<T>, T>(shardToIterator.Value, converter, shardToIterator.Key, combinedState.CancellationToken));
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
                    if (pagingContinuation.Skip > 0)
                    {
                        pagingContinuation.Skip--;
                        continue;
                    }

                    if (pageSize-- <= 0)
                        yield break;

                    var shard = result.ShardNumber;
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

            public IAsyncEnumerable<ShardStreamItem<BlittableJsonReaderObject>> PagedShardedDocumentsBlittableByLastModified(
                CombinedReadContinuationState combinedState,
                string name,
                ShardedPagingContinuation pagingContinuation)
            {
                return PagedShardedStream(
                    combinedState,
                    name,
                    x => x,
                    DocumentLastModifiedComparer.Instance,
                    pagingContinuation);
            }

            public IAsyncEnumerable<ShardStreamItem<BlittableJsonReaderObject>> PagedShardedDocumentsBlittableById(
                CombinedReadContinuationState combinedState,
                string name,
                ShardedPagingContinuation pagingContinuation)
            {
                return PagedShardedStream(
                    combinedState,
                    name,
                    x => x,
                    BlittableIdComparer.Instance,
                    pagingContinuation);
            }

            public sealed class DocumentIdComparer : Comparer<ShardStreamItem<Document>>
            {
                public override int Compare(ShardStreamItem<Document> x, ShardStreamItem<Document> y)
                {
                    if (x == y)
                        return 0;
                    if (x == null)
                        return -1;
                    if (y == null)
                        return 1;
                
                    return x.Item.LowerId.CompareTo(y.Item.LowerId);
                }

                public static DocumentIdComparer Instance = new();
            }

            public sealed class BlittableIdComparer : Comparer<ShardStreamItem<BlittableJsonReaderObject>>
            {
                public override int Compare(ShardStreamItem<BlittableJsonReaderObject> x, ShardStreamItem<BlittableJsonReaderObject> y)
                {
                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "RavenDB-19088 Avoid allocation here");
                    return string.Compare(x.Item.GetMetadata().GetId(), y.Item.GetMetadata().GetId(), StringComparison.OrdinalIgnoreCase);
                }

                public static BlittableIdComparer Instance = new();
            }

            public sealed class DocumentLastModifiedComparer : Comparer<ShardStreamItem<BlittableJsonReaderObject>>
            {
                public override int Compare(ShardStreamItem<BlittableJsonReaderObject> x, ShardStreamItem<BlittableJsonReaderObject> y)
                {
                    return y.Item.GetMetadata().GetLastModified().CompareTo(x.Item.GetMetadata().GetLastModified());
                }

                public static DocumentLastModifiedComparer Instance = new ();
            }

            public sealed class RevisionLastModifiedComparer : Comparer<ShardStreamItem<BlittableJsonReaderObject>>
            {
                public override int Compare(ShardStreamItem<BlittableJsonReaderObject> x, ShardStreamItem<BlittableJsonReaderObject> y)
                {
                    var xLastModified = GetLatModified(x);
                    var yLastModified = GetLatModified(y);

                    return yLastModified.CompareTo(xLastModified);
                }

                private DateTime GetLatModified(ShardStreamItem<BlittableJsonReaderObject> x)
                {
                    if (x.Item.TryGet(nameof(Document.LastModified), out DateTime lastModified) == false)
                        throw new InvalidOperationException($"Revision does not contain 'LastModified' field.");

                    return lastModified;
                }

                public static RevisionLastModifiedComparer Instance = new();
            }

            public IEnumerable<BlittableJsonReaderObject> PagedShardedItemDocumentsByLastModified<TInput>(
                Dictionary<int, ShardExecutionResult<TInput>> results,
                Func<TInput, IEnumerable<BlittableJsonReaderObject>> selector,
                ShardedPagingContinuation pagingContinuation)
            {
                return PagedShardedItem(
                    results,
                    selector,
                    DocumentLastModifiedComparer.Instance,
                    pagingContinuation);
            }

            public IAsyncEnumerable<ShardStreamItem<BlittableJsonReaderObject>> GetDocumentsAsync(CombinedReadContinuationState documents, ShardedPagingContinuation pagingContinuation) =>
                PagedShardedStream(
                documents,
                "Results",
                x => x,
                DocumentLastModifiedComparer.Instance,
                pagingContinuation);

            public IAsyncEnumerable<ShardStreamItem<BlittableJsonReaderObject>> GetDocumentsAsyncById(CombinedReadContinuationState documents, ShardedPagingContinuation pagingContinuation) =>
                PagedShardedStream(
                documents,
                "Results",
                x => x,
                BlittableIdComparer.Instance,
                pagingContinuation);


            public static async IAsyncEnumerable<T> UnwrapDocuments<T>(IAsyncEnumerable<ShardStreamItem<T>> docs,
                ShardedPagingContinuation shardedPagingContinuation)
            {
                await foreach (var doc in docs)
                {
                    if (shardedPagingContinuation.Skip > 0)
                    {
                        shardedPagingContinuation.Skip--;
                        continue;
                    }

                    yield return doc.Item;
                }
            }

            public IEnumerable<T> PagedShardedItem<T, TInput>(
                Dictionary<int, ShardExecutionResult<TInput>> results,
                Func<TInput, IEnumerable<T>> selector,
                Comparer<ShardStreamItem<T>> comparer,
                ShardedPagingContinuation pagingContinuation)
            {
                var pageSize = pagingContinuation.PageSize;
                foreach (var result in CombinedResults(results, selector, comparer))
                {
                    if (pagingContinuation.Skip > 0)
                    {
                        pagingContinuation.Skip--;
                        continue;
                    }

                    if (pageSize-- <= 0)
                        yield break;

                    var shardNumber = result.ShardNumber;
                    pagingContinuation.Pages[shardNumber].Start++;

                    yield return result.Item;
                }
            }

            public IEnumerable<ShardStreamItem<T>> CombinedResults<T, TInput>(
                Dictionary<int, ShardExecutionResult<TInput>> results,
                Func<TInput, IEnumerable<T>> selector,
                Comparer<ShardStreamItem<T>> comparer)
            {
                using (var merged = new MergedEnumerator<ShardStreamItem<T>>(comparer)) 
                {
                    foreach (var (shardNumber, result) in results)
                    {
                        var r = selector(result.Result);
                        var it = r.Select(item => new ShardStreamItem<T>
                        {
                            Item = item,
                            ShardNumber = result.ShardNumber
                        });
                        merged.AddEnumerator(it.GetEnumerator());
                    }

                    while (merged.MoveNext())
                    {
                        yield return merged.Current;
                    }
                }
            }

            private sealed class YieldStreamArray<T, TInner> : AsyncDocumentSession.AbstractYieldStream<T> where T : ShardStreamItem<TInner>
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
                        ShardNumber = _shard
                    };
                }
            }
        }
    }
}
