using System;
using System.Collections.Concurrent;
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

    private readonly ConcurrentDictionary<string, IDisposable> _matchingCounters = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<string, IDisposable> _matchingDocumentCounters = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<DocumentIdAndNamePair, IDisposable> _matchingDocumentCounter = new();

    private int _watchAllDocuments;
    private IDisposable _watchAllDocumentsUnsubscribe;

    private int _watchAllCounters;
    private IDisposable _watchAllCountersUnsubscribe;

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

    private ValueTask EnsureConnectedAsync()
    {
        if (_changes != null)
            return ValueTask.CompletedTask;

        _changes = new ShardedDatabaseChanges[_context.NumberOfShardNodes];
        for (var i = 0; i < _changes.Length; i++)
            _changes[i] = new ShardedDatabaseChanges(_context.ShardExecutor.GetRequestExecutorAt(i), ShardHelper.ToShardName(_context.DatabaseName, i), onDispose: null, nodeTag: null, _throttleConnection);

        return ValueTask.CompletedTask;
    }

    protected override ValueTask WatchTopologyAsync()
    {
        //no-op
        return ValueTask.CompletedTask;
    }

    protected override async ValueTask WatchDocumentAsync(string docId, CancellationToken token)
    {
        await EnsureConnectedAsync();

        _matchingDocuments.GetOrAdd(docId, id => WatchInternalAsync(changes => changes.ForDocument(id), token).Result);
    }

    protected override async ValueTask UnwatchDocumentAsync(string docId, CancellationToken token)
    {
        await EnsureConnectedAsync();

        if (_matchingDocuments.TryRemove(docId, out var unsubscribe) == false)
            return;

        unsubscribe.Dispose();
    }

    protected override async ValueTask WatchAllDocumentsAsync(CancellationToken token)
    {
        await EnsureConnectedAsync();

        var value = Interlocked.Increment(ref _watchAllDocuments);
        if (value == 1)
            _watchAllDocumentsUnsubscribe = await WatchInternalAsync(changes => changes.ForAllDocuments(), token);
    }

    protected override async ValueTask UnwatchAllDocumentsAsync(CancellationToken token)
    {
        await EnsureConnectedAsync();

        var value = Interlocked.Decrement(ref _watchAllDocuments);
        if (value == 0)
        {
            var watchAllDocumentsUnsubscribe = _watchAllDocumentsUnsubscribe;
            watchAllDocumentsUnsubscribe?.Dispose();
        }
    }

    protected override async ValueTask WatchCounterAsync(string name, CancellationToken token)
    {
        await EnsureConnectedAsync();

        _matchingCounters.GetOrAdd(name, n => WatchInternalAsync(changes => changes.ForCounter(n), token).Result);
    }

    protected override async ValueTask UnwatchCounterAsync(string name, CancellationToken token)
    {
        await EnsureConnectedAsync();

        if (_matchingCounters.TryRemove(name, out var unsubscribe) == false)
            return;

        unsubscribe.Dispose();
    }

    protected override async ValueTask WatchDocumentCountersAsync(string docId, CancellationToken token)
    {
        await EnsureConnectedAsync();

        _matchingDocumentCounters.GetOrAdd(docId, id => WatchInternalAsync(changes => changes.ForCountersOfDocument(id), token).Result);
    }

    protected override async ValueTask UnwatchDocumentCountersAsync(string docId, CancellationToken token)
    {
        await EnsureConnectedAsync();

        if (_matchingDocumentCounters.TryRemove(docId, out var unsubscribe) == false)
            return;

        unsubscribe.Dispose();
    }

    protected override async ValueTask WatchDocumentCounterAsync(BlittableJsonReaderArray parameters, CancellationToken token)
    {
        await EnsureConnectedAsync();

        var val = GetParameters(parameters);

        _matchingDocumentCounter.GetOrAdd(val, v => WatchInternalAsync(changes => changes.ForCounterOfDocument(v.DocumentId, v.Name), token).Result);
    }

    protected override async ValueTask UnwatchDocumentCounterAsync(BlittableJsonReaderArray parameters, CancellationToken token)
    {
        await EnsureConnectedAsync();

        var val = GetParameters(parameters);

        if (_matchingDocumentCounter.TryRemove(val, out var unsubscribe) == false)
            return;

        unsubscribe.Dispose();
    }

    protected override async ValueTask WatchAllCountersAsync(CancellationToken token)
    {
        await EnsureConnectedAsync();

        var value = Interlocked.Increment(ref _watchAllCounters);
        if (value == 1)
            _watchAllCountersUnsubscribe = await WatchInternalAsync(changes => changes.ForAllCounters(), token);
    }

    protected override async ValueTask UnwatchAllCountersAsync(CancellationToken token)
    {
        await EnsureConnectedAsync();

        var value = Interlocked.Decrement(ref _watchAllCounters);
        if (value == 0)
        {
            var watchAllCountersUnsubscribe = _watchAllCountersUnsubscribe;
            watchAllCountersUnsubscribe?.Dispose();
        }
    }

    protected override ValueTask WatchTimeSeriesAsync(string name)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask UnwatchTimeSeriesAsync(string name)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask WatchAllDocumentTimeSeriesAsync(string docId)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask UnwatchAllDocumentTimeSeriesAsync(string docId)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask WatchDocumentTimeSeriesAsync(BlittableJsonReaderArray parameters)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask UnwatchDocumentTimeSeriesAsync(BlittableJsonReaderArray parameters)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask WatchAllTimeSeriesAsync()
    {
        throw new NotImplementedException();
    }

    protected override ValueTask UnwatchAllTimeSeriesAsync()
    {
        throw new NotImplementedException();
    }

    protected override ValueTask WatchDocumentPrefixAsync(string name)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask UnwatchDocumentPrefixAsync(string name)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask WatchDocumentInCollectionAsync(string name)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask UnwatchDocumentInCollectionAsync(string name)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask WatchDocumentOfTypeAsync(string name)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask UnwatchDocumentOfTypeAsync(string name)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask WatchAllIndexesAsync()
    {
        throw new NotImplementedException();
    }

    protected override ValueTask UnwatchAllIndexesAsync()
    {
        throw new NotImplementedException();
    }

    protected override ValueTask WatchIndexAsync(string name)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask UnwatchIndexAsync(string name)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask WatchOperationAsync(long operationId)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask UnwatchOperationAsync(long operationId)
    {
        throw new NotImplementedException();
    }

    protected override ValueTask WatchAllOperationsAsync()
    {
        throw new NotImplementedException();
    }

    protected override ValueTask UnwatchAllOperationsAsync()
    {
        throw new NotImplementedException();
    }

    public override DynamicJsonValue GetDebugInfo()
    {
        throw new NotImplementedException();
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
        foreach (var changes in _changes)
            changes.Dispose();

        _changes = null;

        base.Dispose();

        _queueContext = null;
        _releaseQueueContext?.Dispose();
        _releaseQueueContext = null;
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

    private readonly struct MultiDispose : IDisposable
    {
        private readonly IDisposable[] _toDispose;

        public MultiDispose([NotNull] IDisposable[] toDispose)
        {
            _toDispose = toDispose ?? throw new ArgumentNullException(nameof(toDispose));
        }

        public void Dispose()
        {
            foreach (var item in _toDispose)
                item.Dispose();
        }
    }
}
