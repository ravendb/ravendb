using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.Outgoing;
using Xunit;

namespace SlowTests.Issues.RavenDB_26295.Tools;

public sealed class LogicalInternalReplicationBlocker : IAsyncDisposable
{
    private readonly DocumentDatabase _database;
    private readonly string _fromNodeTag;
    private readonly string _toNodeTag;
    private readonly ManualResetEventSlim _gate = new(initialState: false);
    private readonly ManualResetEventSlim _entered = new(initialState: false);
    private readonly object _locker = new();
    private readonly HashSet<DatabaseOutgoingReplicationHandler> _attachedHandlers = [];
    private readonly Dictionary<DatabaseOutgoingReplicationHandler, Action> _previousFetchCallbacks = new();
    private Action<DatabaseOutgoingReplicationHandler> _previousOutgoingReplicationStart;
    private int _waiting;
    private bool _disposed;
    private bool _released;

    public LogicalInternalReplicationBlocker(DocumentDatabase database, string fromNodeTag, string toNodeTag)
    {
        _database = database;
        _fromNodeTag = fromNodeTag;
        _toNodeTag = toNodeTag;
    }

    public async Task AttachAsync()
    {
        var loaderTesting = _database.ReplicationLoader.ForTestingPurposesOnly();
        _previousOutgoingReplicationStart = loaderTesting.OnOutgoingReplicationStart;
        loaderTesting.OnOutgoingReplicationStart = OnOutgoingReplicationStart;

        await AttachToExistingHandlersAsync();
    }

    public Task WaitForBlockedAsync()
    {
        var blocked = _entered.Wait(TimeSpan.FromSeconds(10));

        Assert.True(blocked, $"Expected {_fromNodeTag}->{_toNodeTag} internal replication blocker to attach.");
        return Task.CompletedTask;
    }

    public void AssertStillBlocking()
    {
        Assert.True(_entered.IsSet, $"Expected {_fromNodeTag}->{_toNodeTag} internal replication blocker to be active.");
        Assert.False(Volatile.Read(ref _released), $"Expected {_fromNodeTag}->{_toNodeTag} internal replication blocker to remain blocked.");
        Assert.False(Volatile.Read(ref _disposed), $"Expected {_fromNodeTag}->{_toNodeTag} internal replication blocker not to be disposed.");
        Assert.True(
            Volatile.Read(ref _waiting) > 0,
            $"Expected {_fromNodeTag}->{_toNodeTag} internal replication blocker to have a replication handler parked at the gate.");
    }

    private async Task AttachToExistingHandlersAsync()
    {
        var databaseReplicationLoader = _database.ReplicationLoader;
        var handlers = databaseReplicationLoader.OutgoingHandlers
            .Where(Matches)
            .ToList();

        foreach (var handler in handlers)
            AttachHandler(handler);

        await Task.CompletedTask;
    }

    private void OnOutgoingReplicationStart(DatabaseOutgoingReplicationHandler handler)
    {
        _previousOutgoingReplicationStart?.Invoke(handler);

        if (Matches(handler) == false)
            return;

        AttachHandler(handler);
    }

    public bool Matches(string fromNodeTag, string toNodeTag) =>
        string.Equals(_fromNodeTag, fromNodeTag, StringComparison.OrdinalIgnoreCase) &&
        string.Equals(_toNodeTag, toNodeTag, StringComparison.OrdinalIgnoreCase);

    private bool Matches(DatabaseOutgoingReplicationHandler handler)
    {
        return handler.Destination is InternalReplication internalReplication && string.Equals(internalReplication.NodeTag, _toNodeTag, StringComparison.OrdinalIgnoreCase);
    }

    private void AttachHandler(DatabaseOutgoingReplicationHandler handler)
    {
        lock (_locker)
        {
            if (_disposed)
                return;

            if (_attachedHandlers.Add(handler) == false)
                return;

            _previousFetchCallbacks[handler] = handler.ForTestingPurposesOnly().OnDocumentSenderFetchNewItem;
            handler.ForTestingPurposesOnly().OnDocumentSenderFetchNewItem = () => WaitForFetch(handler);
        }
    }

    private void WaitForFetch(DatabaseOutgoingReplicationHandler handler)
    {
        Action previousFetch;

        lock (_locker)
            _previousFetchCallbacks.TryGetValue(handler, out previousFetch);

        previousFetch?.Invoke();

        if (Volatile.Read(ref _released))
        {
            _entered.Set();
            return;
        }

        Interlocked.Increment(ref _waiting);
        _entered.Set();

        try
        {
            _gate.Wait();
        }
        finally
        {
            Interlocked.Decrement(ref _waiting);
        }
    }

    private void Release()
    {
        Volatile.Write(ref _released, true);
        _gate.Set();
    }

    public async ValueTask DisposeAsync()
    {
        List<KeyValuePair<DatabaseOutgoingReplicationHandler, Action>> handlersToRestore;

        lock (_locker)
        {
            if (_disposed)
                return;

            _disposed = true;
            handlersToRestore = new List<KeyValuePair<DatabaseOutgoingReplicationHandler, Action>>(_previousFetchCallbacks);
            _previousFetchCallbacks.Clear();
        }

        var loaderTesting = _database.ReplicationLoader.ForTestingPurposesOnly();
        loaderTesting.OnOutgoingReplicationStart = _previousOutgoingReplicationStart;

        foreach (var handlerToRestore in handlersToRestore)
            handlerToRestore.Key.ForTestingPurposesOnly().OnDocumentSenderFetchNewItem = handlerToRestore.Value;

        Release();
        _entered.Set();
        _gate.Dispose();
        _entered.Dispose();
        await Task.CompletedTask;
    }
}
