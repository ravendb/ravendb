using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Changes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Changes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Changes;

public class ShardedChangesClientConnection : AbstractChangesClientConnection<TransactionOperationContext>, IObserver<BlittableJsonReaderObject>
{
    private readonly ConcurrentDictionary<string, IDisposable> _matchingDocuments = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<string, IDisposable> _matchingIndexes = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<string, IDisposable> _matchingDocumentPrefixes = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<string, IDisposable> _matchingDocumentsInCollection = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<string, IDisposable> _matchingCounters = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<string, IDisposable> _matchingDocumentCounters = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<DocumentIdAndNamePair, IDisposable> _matchingDocumentCounter = new();

    private readonly ConcurrentDictionary<string, IDisposable> _matchingTimeSeries = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<string, IDisposable> _matchingAllDocumentTimeSeries = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<DocumentIdAndNamePair, IDisposable> _matchingDocumentTimeSeries = new();

    private int _watchAllDocuments;
    private IDisposable _watchAllDocumentsUnsubscribe;

    private int _watchAllCounters;
    private IDisposable _watchAllCountersUnsubscribe;

    private int _watchAllTimeSeries;
    private IDisposable _watchAllTimeSeriesUnsubscribe;

    private int _watchAllIndexes;
    private IDisposable _watchAllIndexesUnsubscribe;

    private readonly ShardedDatabaseContext _context;
    private readonly bool _throttleConnection;
    private ShardedDatabaseChanges[] _changes;

    private IDisposable _releaseQueueContext;
    private JsonOperationContext _queueContext;

    public ShardedChangesClientConnection(WebSocket webSocket, ServerStore serverStore, [NotNull] ShardedDatabaseContext context, bool throttleConnection, bool fromStudio)
        : base(webSocket, serverStore.ContextPool, context.DatabaseShutdown, throttleConnection: false, fromStudio)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _throttleConnection = throttleConnection;
        _releaseQueueContext = ContextPool.AllocateOperationContext(out _queueContext);
    }

    private async ValueTask EnsureConnectedAsync(CancellationToken token)
    {
        if (_changes != null)
            return;

        var tasks = new Task[_context.NumberOfShardNodes];
        _changes = new ShardedDatabaseChanges[_context.NumberOfShardNodes];
        for (var i = 0; i < _changes.Length; i++)
        {
            _changes[i] = new ShardedDatabaseChanges(_context.ShardExecutor.GetRequestExecutorAt(i), ShardHelper.ToShardName(_context.DatabaseName, i), onDispose: null, nodeTag: null, _throttleConnection);
            tasks[i] = _changes[i].EnsureConnectedNow();
        }

        await Task.WhenAll(tasks).WithCancellation(token);
    }

    protected override async ValueTask WatchDocumentAsync(string docId, CancellationToken token)
    {
        await EnsureConnectedAsync(token);

        _matchingDocuments.GetOrAdd(docId, id => WatchInternalAsync(changes => changes.ForDocument(id), token).Result);
    }

    protected override ValueTask UnwatchDocumentAsync(string docId, CancellationToken token)
    {
        return UnwatchInternalAsync(docId, _matchingDocuments, token);
    }

    protected override async ValueTask WatchAllDocumentsAsync(CancellationToken token)
    {
        await EnsureConnectedAsync(token);

        var value = Interlocked.Increment(ref _watchAllDocuments);
        if (value == 1)
            _watchAllDocumentsUnsubscribe = await WatchInternalAsync(changes => changes.ForAllDocuments(), token);
    }

    protected override ValueTask UnwatchAllDocumentsAsync(CancellationToken token)
    {
        UnwatchInternal(ref _watchAllDocuments, _watchAllDocumentsUnsubscribe);
        return ValueTask.CompletedTask;
    }

    protected override async ValueTask WatchCounterAsync(string name, CancellationToken token)
    {
        await EnsureConnectedAsync(token);

        _matchingCounters.GetOrAdd(name, n => WatchInternalAsync(changes => changes.ForCounter(n), token).Result);
    }

    protected override ValueTask UnwatchCounterAsync(string name, CancellationToken token)
    {
        return UnwatchInternalAsync(name, _matchingCounters, token);
    }

    protected override async ValueTask WatchDocumentCountersAsync(string docId, CancellationToken token)
    {
        await EnsureConnectedAsync(token);

        _matchingDocumentCounters.GetOrAdd(docId, id => WatchInternalAsync(changes => changes.ForCountersOfDocument(id), token).Result);
    }

    protected override ValueTask UnwatchDocumentCountersAsync(string docId, CancellationToken token)
    {
        return UnwatchInternalAsync(docId, _matchingDocumentCounters, token);
    }

    protected override async ValueTask WatchDocumentCounterAsync(BlittableJsonReaderArray parameters, CancellationToken token)
    {
        await EnsureConnectedAsync(token);

        var val = GetParameters(parameters);

        _matchingDocumentCounter.GetOrAdd(val, v => WatchInternalAsync(changes => changes.ForCounterOfDocument(v.DocumentId, v.Name), token).Result);
    }

    protected override ValueTask UnwatchDocumentCounterAsync(BlittableJsonReaderArray parameters, CancellationToken token)
    {
        var val = GetParameters(parameters);

        return UnwatchInternalAsync(val, _matchingDocumentCounter, token);
    }

    protected override async ValueTask WatchAllCountersAsync(CancellationToken token)
    {
        await EnsureConnectedAsync(token);

        var value = Interlocked.Increment(ref _watchAllCounters);
        if (value == 1)
            _watchAllCountersUnsubscribe = await WatchInternalAsync(changes => changes.ForAllCounters(), token);
    }

    protected override ValueTask UnwatchAllCountersAsync(CancellationToken token)
    {
        UnwatchInternal(ref _watchAllCounters, _watchAllCountersUnsubscribe);
        return ValueTask.CompletedTask;
    }

    protected override async ValueTask WatchTimeSeriesAsync(string name, CancellationToken token)
    {
        await EnsureConnectedAsync(token);

        _matchingTimeSeries.GetOrAdd(name, n => WatchInternalAsync(changes => changes.ForTimeSeries(n), token).Result);
    }

    protected override ValueTask UnwatchTimeSeriesAsync(string name, CancellationToken token)
    {
        return UnwatchInternalAsync(name, _matchingTimeSeries, token);
    }

    protected override async ValueTask WatchAllDocumentTimeSeriesAsync(string docId, CancellationToken token)
    {
        await EnsureConnectedAsync(token);

        _matchingAllDocumentTimeSeries.GetOrAdd(docId, id => WatchInternalAsync(changes => changes.ForTimeSeriesOfDocument(id), token).Result);
    }

    protected override ValueTask UnwatchAllDocumentTimeSeriesAsync(string docId, CancellationToken token)
    {
        return UnwatchInternalAsync(docId, _matchingAllDocumentTimeSeries, token);
    }

    protected override async ValueTask WatchDocumentTimeSeriesAsync(BlittableJsonReaderArray parameters, CancellationToken token)
    {
        await EnsureConnectedAsync(token);

        var val = GetParameters(parameters);

        _matchingDocumentTimeSeries.GetOrAdd(val, v => WatchInternalAsync(changes => changes.ForTimeSeriesOfDocument(v.DocumentId, v.Name), token).Result);
    }

    protected override ValueTask UnwatchDocumentTimeSeriesAsync(BlittableJsonReaderArray parameters, CancellationToken token)
    {
        var val = GetParameters(parameters);

        return UnwatchInternalAsync(val, _matchingDocumentTimeSeries, token);
    }

    protected override async ValueTask WatchAllTimeSeriesAsync(CancellationToken token)
    {
        await EnsureConnectedAsync(token);

        var value = Interlocked.Increment(ref _watchAllTimeSeries);
        if (value == 1)
            _watchAllTimeSeriesUnsubscribe = await WatchInternalAsync(changes => changes.ForAllTimeSeries(), token);
    }

    protected override ValueTask UnwatchAllTimeSeriesAsync(CancellationToken token)
    {
        UnwatchInternal(ref _watchAllTimeSeries, _watchAllTimeSeriesUnsubscribe);
        return ValueTask.CompletedTask;
    }

    protected override async ValueTask WatchDocumentPrefixAsync(string name, CancellationToken token)
    {
        await EnsureConnectedAsync(token);

        _matchingDocumentPrefixes.GetOrAdd(name, p => WatchInternalAsync(changes => changes.ForDocumentsStartingWith(p), token).Result);
    }

    protected override ValueTask UnwatchDocumentPrefixAsync(string name, CancellationToken token)
    {
        return UnwatchInternalAsync(name, _matchingDocumentPrefixes, token);
    }

    protected override async ValueTask WatchDocumentInCollectionAsync(string name, CancellationToken token)
    {
        await EnsureConnectedAsync(token);

        _matchingDocumentsInCollection.GetOrAdd(name, c => WatchInternalAsync(changes => changes.ForDocumentsInCollection(c), token).Result);
    }

    protected override ValueTask UnwatchDocumentInCollectionAsync(string name, CancellationToken token)
    {
        return UnwatchInternalAsync(name, _matchingDocumentsInCollection, token);
    }

    protected override async ValueTask WatchAllIndexesAsync(CancellationToken token)
    {
        await EnsureConnectedAsync(token);

        var value = Interlocked.Increment(ref _watchAllIndexes);
        if (value == 1)
            _watchAllIndexesUnsubscribe = await WatchInternalAsync(changes => changes.ForAllIndexes(), token);
    }

    protected override ValueTask UnwatchAllIndexesAsync(CancellationToken token)
    {
        UnwatchInternal(ref _watchAllIndexes, _watchAllIndexesUnsubscribe);
        return ValueTask.CompletedTask;
    }

    protected override async ValueTask WatchIndexAsync(string name, CancellationToken token)
    {
        await EnsureConnectedAsync(token);

        _matchingIndexes.GetOrAdd(name, n => WatchInternalAsync(changes => changes.ForIndex(n), token).Result);
    }

    protected override ValueTask UnwatchIndexAsync(string name, CancellationToken token)
    {
        return UnwatchInternalAsync(name, _matchingIndexes, token);
    }

    public override DynamicJsonValue GetDebugInfo()
    {
        var djv = base.GetDebugInfo();

        djv["WatchAllDocuments"] = _watchAllDocuments > 0;
        djv["WatchAllIndexes"] = _watchAllIndexes > 0;
        djv["WatchAllCounters"] = _watchAllCounters > 0;
        djv["WatchAllTimeSeries"] = _watchAllTimeSeries > 0;
        djv["WatchDocumentPrefixes"] = _matchingDocumentPrefixes.ToArray();
        djv["WatchDocumentsInCollection"] = _matchingDocumentsInCollection.ToArray();
        djv["WatchIndexes"] = _matchingIndexes.ToArray();
        djv["WatchDocuments"] = _matchingDocuments.ToArray();
        djv["WatchCounters"] = _matchingCounters.ToArray();
        djv["WatchCounterOfDocument"] = _matchingDocumentCounter.Select(x => x.Key.ToJson()).ToArray();
        djv["WatchCountersOfDocument"] = _matchingDocumentCounters.ToArray();
        djv["WatchTimeSeries"] = _matchingTimeSeries.ToArray();
        djv["WatchTimeSeriesOfDocument"] = _matchingDocumentTimeSeries.Select(x => x.Key.ToJson()).ToArray();
        djv["WatchAllTimeSeriesOfDocument"] = _matchingAllDocumentTimeSeries.ToArray();

        return djv;
    }

    public void OnCompleted()
    {
    }

    public void OnError(Exception error)
    {
    }

    public void OnNext(BlittableJsonReaderObject value)
    {
        using (value)
        {
            AddToQueue(new SendQueueItem
            {
                ValueToSend = value.Clone(_queueContext)
            });
        }
    }

    public override void Dispose()
    {
        var exceptionAggregator = new ExceptionAggregator($"Could not dispose '{_context.DatabaseName}' changes.");
        foreach (var changes in _changes)
            exceptionAggregator.Execute(changes);

        _changes = null;

        base.Dispose();

        _queueContext = null;
        _releaseQueueContext?.Dispose();
        _releaseQueueContext = null;

        exceptionAggregator.ThrowIfNeeded();
    }

    private async Task<IDisposable> WatchInternalAsync(Func<ShardedDatabaseChanges, IChangesObservable<BlittableJsonReaderObject>> factory, CancellationToken token)
    {
        var toDispose = new IDisposable[_changes.Length];
        for (int i = 0; i < _changes.Length; i++)
        {
            var observable = factory(_changes[i]);
            toDispose[i] = observable
                .Subscribe(this);

            await observable.EnsureSubscribedNow().WithCancellation(token);
        }

        return new MultiDispose(toDispose);
    }

    private async ValueTask UnwatchInternalAsync<TKey>(TKey key, ConcurrentDictionary<TKey, IDisposable> subscriptions, CancellationToken token)
    {
        await EnsureConnectedAsync(token);

        if (subscriptions.TryRemove(key, out var unsubscribe) == false)
            return;

        unsubscribe.Dispose();
    }

    private static void UnwatchInternal(ref int watch, IDisposable unsubscribe)
    {
        var value = Interlocked.Decrement(ref watch);
        if (value == 0)
        {
            unsubscribe?.Dispose();
        }
    }

    private readonly struct MultiDispose : IDisposable
    {
        private readonly IDisposable[] _toDispose;

        public MultiDispose([NotNull] IDisposable[] toDispose)
        {
            _toDispose = toDispose ?? throw new ArgumentNullException(nameof(toDispose));
        }

        public void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator("Could not unsubscribe from changes.");
            foreach (var item in _toDispose)
                exceptionAggregator.Execute(item);

            exceptionAggregator.ThrowIfNeeded();
        }
    }
}
