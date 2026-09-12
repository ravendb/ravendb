using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.Outgoing;
using Xunit;

namespace SlowTests.Issues.RavenDB_26295.Tools;

public sealed class FilteredRoundTripReplicationGate : IDisposable
{
    private readonly DocumentDatabase _database;
    private readonly Func<DatabaseOutgoingReplicationHandler, bool> _matches;
    private readonly string _description;
    private readonly ManualResetEventSlim _entered = new(initialState: false);
    private readonly ManualResetEventSlim _release = new(initialState: false);
    private readonly object _locker = new();
    private readonly HashSet<DatabaseOutgoingReplicationHandler> _attachedHandlers = [];
    private readonly Dictionary<DatabaseOutgoingReplicationHandler, Action<DatabaseOutgoingReplicationHandler>> _previousCallbacks = new();
    private Action<DatabaseOutgoingReplicationHandler> _previousOutgoingReplicationStart;
    private bool _disposed;
    private bool _released;

    public FilteredRoundTripReplicationGate(
        DocumentDatabase database,
        Func<DatabaseOutgoingReplicationHandler, bool> matches,
        string description)
    {
        _database = database;
        _matches = matches;
        _description = description;
    }

    public void Attach()
    {
        var loaderTesting = _database.ReplicationLoader.ForTestingPurposesOnly();
        _previousOutgoingReplicationStart = loaderTesting.OnOutgoingReplicationStart;
        loaderTesting.OnOutgoingReplicationStart = OnOutgoingReplicationStart;

        foreach (var handler in _database.ReplicationLoader.OutgoingHandlers.Where(_matches).ToList())
            AttachHandler(handler);
    }

    public Task WaitForBlockedAsync()
    {
        var blocked = _entered.Wait(TimeSpan.FromSeconds(10));

        Assert.True(blocked, $"Expected {_description} handler to reach the filtered round-trip gate.");
        return Task.CompletedTask;
    }

    public void Release()
    {
        _released = true;
        _release.Set();
    }

    private void OnOutgoingReplicationStart(DatabaseOutgoingReplicationHandler handler)
    {
        _previousOutgoingReplicationStart?.Invoke(handler);

        if (_matches(handler) == false)
            return;

        AttachHandler(handler);
    }

    private void AttachHandler(DatabaseOutgoingReplicationHandler handler)
    {
        lock (_locker)
        {
            if (_disposed)
                return;

            if (_attachedHandlers.Add(handler) == false)
                return;

            var handlerTesting = handler.ForTestingPurposesOnly();
            _previousCallbacks[handler] = handlerTesting.BeforeExecuteReplicationOnce;
            handlerTesting.BeforeExecuteReplicationOnce = WaitBeforeScan;
        }
    }

    private void WaitBeforeScan(DatabaseOutgoingReplicationHandler handler)
    {
        Action<DatabaseOutgoingReplicationHandler> previous;

        lock (_locker)
            _previousCallbacks.TryGetValue(handler, out previous);

        previous?.Invoke(handler);
        _entered.Set();

        if (_released == false)
            _release.Wait();
    }

    public void Dispose()
    {
        List<KeyValuePair<DatabaseOutgoingReplicationHandler, Action<DatabaseOutgoingReplicationHandler>>> handlersToRestore;

        lock (_locker)
        {
            if (_disposed)
                return;

            _disposed = true;
            handlersToRestore = new List<KeyValuePair<DatabaseOutgoingReplicationHandler, Action<DatabaseOutgoingReplicationHandler>>>(_previousCallbacks);
            _previousCallbacks.Clear();
        }

        var loaderTesting = _database.ReplicationLoader.ForTestingPurposesOnly();
        loaderTesting.OnOutgoingReplicationStart = _previousOutgoingReplicationStart;

        foreach (var handlerToRestore in handlersToRestore)
            handlerToRestore.Key.ForTestingPurposesOnly().BeforeExecuteReplicationOnce = handlerToRestore.Value;

        Release();
        _entered.Set();
        _entered.Dispose();
        _release.Dispose();
    }
}
