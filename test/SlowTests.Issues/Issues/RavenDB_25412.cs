using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Extensions;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25412 : ReplicationTestBase
{
    public RavenDB_25412(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task HubSinkPullReplication_Should_Reconnect_When_Remote_Stops_Sending(Options options)
    {
        DoNotReuseServer();

        using var hubServer = GetNewServer();
        using var sinkServer = GetNewServer();

        options.Server = hubServer;
        using var hubStore = GetDocumentStore(options);

        options.Server = sinkServer;
        using var sinkStore = GetDocumentStore(options);

        var pullReplicationName = $"{hubStore.Database}-pull";
        var connectionStringName = "ConnectionString-" + hubStore.Database;

        var sinkDb = await GetDatabase(sinkStore.Database, sinkServer);

        // Controls the network simulation (Open = normal, Closed = hang)
        var networkGate = new AsyncGate();
        int connectionCounter = 0;

        var forTesting = sinkDb.ReplicationLoader.ForTestingPurposesOnly();

        // Inject the wrapper to simulate network hangs on the socket stream
        forTesting.WrapIncomingReplicationStream = innerStream =>
        {
            Interlocked.Increment(ref connectionCounter);
            return new SmartHangingStreamWrapper(innerStream, networkGate);
        };

        await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(pullReplicationName)));
        await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
        {
            Name = connectionStringName,
            Database = hubStore.Database,
            TopologyDiscoveryUrls = hubStore.Urls
        }));
        await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
        {
            Name = pullReplicationName,
            HubName = pullReplicationName,
            ConnectionStringName = connectionStringName
        }));

        // 1. Verify Initial Replication works
        using (var session = hubStore.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Id = "Users/1-A", Name = "Lev" });
            await session.SaveChangesAsync();
        }
        Assert.True(WaitForDocument<User>(sinkStore, "Users/1-A", u => u.Name == "Lev"), "Initial replication failed");

        // 2. Simulate Network Hang
        // Any subsequent read on the existing stream will now hang indefinitely until we open the gate.
        var initialConnections = connectionCounter;
        networkGate.Close();

        // 3. Write second document to Hub
        using (var session = hubStore.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Id = "Users/2-A", Name = "Lev" });
            await session.SaveChangesAsync();
        }

        // 1. The existing connection hangs in ReadAsync.
        // 2. The finite timeout triggers in AbstractIncomingReplicationHandler.
        // 3. The socket is disposed.
        // 4. The Sink attempts to reconnect.

        // Wait enough time for the timeout to trigger and the old connection to die.
        var timeout = (int)sinkDb.Configuration.Replication.ActiveConnectionTimeout.AsTimeSpan.Add(TimeSpan.FromSeconds(10)).TotalMilliseconds;
        var isSecondDocReplicated = WaitForDocument<User>(sinkStore, "Users/2-A", u => u.Name == "Lev", timeout);
        Assert.False(isSecondDocReplicated, "Network gate is not actually closed as intended. Test is broken");

        // Open the gate.
        // If the Sink has already created a NEW connection, it might be waiting at the gate.
        // Opening it allows the handshake to proceed.
        networkGate.Open();

        isSecondDocReplicated = WaitForDocument<User>(sinkStore, "Users/2-A", u => u.Name == "Lev", timeout: timeout);
        Assert.True(isSecondDocReplicated, "Should have timed out the hung connection, reconnected, and replicated.");

        Assert.True(connectionCounter > initialConnections, $"Expected reconnection to occur. Initial connections: {initialConnections}, Current connections: {connectionCounter}. If these are equal, the zombie connection was not killed.");
    }

    private class AsyncGate
    {
        private readonly object _lock = new object();
        private TaskCompletionSource<object> _openTcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        private TaskCompletionSource<object> _closeTcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        private volatile bool _isOpen = true;

        public AsyncGate()
        {
            _openTcs.SetResult(null);
        }

        public bool IsOpen => _isOpen;

        public void Close()
        {
            lock (_lock)
            {
                if (!_isOpen) return;
                _isOpen = false;
                // Reset open TCS to a new unsignaled one
                _openTcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                // Signal close TCS
                _closeTcs.TrySetResult(null);
            }
        }

        public void Open()
        {
            lock (_lock)
            {
                if (_isOpen) return;
                _isOpen = true;
                // Reset close TCS to a new unsignaled one
                _closeTcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                // Signal open TCS
                _openTcs.TrySetResult(null);
            }
        }

        public Task WaitToOpenAsync(CancellationToken token)
        {
            lock (_lock)
            {
                if (_isOpen) return Task.CompletedTask;
                return _openTcs.Task.WithCancellation(token);
            }
        }

        public Task WaitToCloseAsync(CancellationToken token)
        {
            lock (_lock)
            {
                if (!_isOpen) return Task.CompletedTask;
                return _closeTcs.Task.WithCancellation(token);
            }
        }
    }

    private class SmartHangingStreamWrapper : Stream
    {
        private readonly Stream _inner;
        private readonly AsyncGate _gate;
        private readonly CancellationTokenSource _disposeCts = new CancellationTokenSource();

        public SmartHangingStreamWrapper(Stream inner, AsyncGate gate)
        {
            _inner = inner;
            _gate = gate;
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Link the caller's token with our dispose token to handle stream disposal correctly
                using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCts.Token))
                {
                    try
                    {
                        // If the gate is closed, wait for it to open.
                        // This handles new connections created while the network is simulated as "down".
                        await _gate.WaitToOpenAsync(linkedCts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        if (_disposeCts.IsCancellationRequested) throw new IOException("Stream disposed");
                        throw;
                    }

                    // Gate is open. Start reading, but also watch for the gate closing mid-read.
                    var readTask = _inner.ReadAsync(buffer, offset, count, cancellationToken);
                    var closeGateTask = _gate.WaitToCloseAsync(linkedCts.Token);

                    var completedTask = await Task.WhenAny(readTask, closeGateTask);
                    if (completedTask == closeGateTask)
                    {
                        // Gate closed during the read operation — simulating mid-stream hang.
                        // Discard the current read result (simulating packet loss) and loop back
                        // to wait for the gate to reopen.
                        continue;
                    }

                    return await readTask;
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _disposeCts.Cancel();
                _inner.Dispose();
                _disposeCts.Dispose();
            }
            base.Dispose(disposing);
        }

        // Boilerplate overrides
        public override void Flush() => _inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
        public override void SetLength(long value) => _inner.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);
        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => _inner.CanSeek;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => _inner.Length;
        public override long Position { get => _inner.Position; set => _inner.Position = value; }
    }
}
