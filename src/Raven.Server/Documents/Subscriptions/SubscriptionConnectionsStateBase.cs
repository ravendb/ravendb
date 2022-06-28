using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Collections;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Voron;

namespace Raven.Server.Documents.Subscriptions;


public abstract class SubscriptionConnectionsStateBase
{
    private AsyncManualResetEvent _waitForMoreDocuments;
    public readonly CancellationTokenSource CancellationTokenSource;

    protected SubscriptionConnectionsStateBase(CancellationToken token)
    {
        CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token);
        _waitForMoreDocuments = new AsyncManualResetEvent(CancellationTokenSource.Token);
    }

    public void NotifyHasMoreDocs()
    {
        while (true)
        {
            var current = _waitForMoreDocuments;
            var last = Interlocked.CompareExchange(ref _waitForMoreDocuments, new AsyncManualResetEvent(CancellationTokenSource.Token), current);
            last.Set();
            
            if (last == current)
                break;
        }
    }

    public Task<bool> WaitForMoreDocs() => _waitForMoreDocuments.WaitAsync();
}

public abstract class SubscriptionConnectionsStateBase<TSubscriptionConnection> : SubscriptionConnectionsStateBase, IDisposable
    where TSubscriptionConnection : SubscriptionConnectionBase
{
    protected readonly ServerStore _server;
    private readonly string _databaseName;
    private readonly long _subscriptionId;
    private SubscriptionState _subscriptionState;
    protected ConcurrentSet<TSubscriptionConnection> _connections;
    protected string _subscriptionName;
    private int _maxConcurrentConnections;

    public string Query;
    public string SubscriptionName => _subscriptionName;
    public long SubscriptionId => _subscriptionId;
    public SubscriptionState SubscriptionState => _subscriptionState;

    public bool IsConcurrent => _connections.FirstOrDefault()?.Strategy == SubscriptionOpeningStrategy.Concurrent;

    private readonly ConcurrentSet<TSubscriptionConnection> _pendingConnections = new ();
    private readonly ConcurrentQueue<TSubscriptionConnection> _recentConnections = new ();
    private readonly ConcurrentQueue<TSubscriptionConnection> _rejectedConnections = new ();
    public IEnumerable<TSubscriptionConnection> RecentConnections => _recentConnections;
    public IEnumerable<TSubscriptionConnection> RecentRejectedConnections => _rejectedConnections;
    public ConcurrentSet<TSubscriptionConnection> PendingConnections => _pendingConnections;


    private readonly SemaphoreSlim _subscriptionActivelyWorkingLock;

    public string PreviouslyRecordedChangeVector;

    protected SubscriptionConnectionsStateBase(ServerStore server, string databaseName, long subscriptionId, CancellationToken token) : base(token)
    {
        if (databaseName.Contains('$'))
            throw new ArgumentException($"Database name {databaseName} can't have the shard name with '$'");

        _server = server;
        _databaseName = databaseName;
        _subscriptionId = subscriptionId;
        _connections = new ConcurrentSet<TSubscriptionConnection>();
        _subscriptionActivelyWorkingLock = new SemaphoreSlim(1);
    }


    public abstract Task UpdateClientConnectionTime();

    public abstract Task WaitForIndexNotificationAsync(long index);

    public abstract void DropSubscription(SubscriptionException e);

    public virtual void Initialize(TSubscriptionConnection connection, bool afterSubscribe = false)
    {
        _subscriptionName = connection.Options.SubscriptionName ?? _subscriptionId.ToString();
        _subscriptionState = connection.SubscriptionState;
        Query = connection.SubscriptionState.Query;
    }

    public virtual async Task<(IDisposable DisposeOnDisconnect, long RegisterConnectionDurationInTicks)> SubscribeAsync(TSubscriptionConnection connection)
    {
        var random = new Random();
        var registerConnectionDuration = Stopwatch.StartNew();

        PendingConnections.Add(connection);
        connection.RecordConnectionInfo();

        try
        {
            while (true)
            {
                CancellationTokenSource.Token.ThrowIfCancellationRequested();

                try
                {
                    var disposeOnce = RegisterSubscriptionConnection(connection);
                    registerConnectionDuration.Stop();
                    return (disposeOnce, registerConnectionDuration.ElapsedTicks);
                }
                catch (TimeoutException)
                {
                    if (connection._logger.IsInfoEnabled)
                    {
                        connection._logger.Info(
                            $"A connection from IP {connection.ClientUri} is starting to wait until previous connection from " +
                            $"{GetConnectionsAsString()} is released");
                    }

                    var timeout = TimeSpan.FromMilliseconds(Math.Max(250, (long)connection.Options.TimeToWaitBeforeConnectionRetry.TotalMilliseconds / 2) + random.Next(15, 50));
                    await Task.Delay(timeout, connection.CancellationTokenSource.Token);
                    await connection.SendHeartBeatAsync(
                        $"A connection from IP {connection.ClientUri} is waiting for Subscription Task that is serving a connection from IP " +
                        $"{GetConnectionsAsString()} to be released");
                }
            }
        }
        finally
        {
            PendingConnections.TryRemove(connection);
        }
    }

    public SubscriptionConnectionsDetails GetSubscriptionConnectionsDetails()
    {
        var subscriptionConnectionsDetails = new SubscriptionConnectionsDetails
        {
            Results = new List<SubscriptionConnectionDetails>()
        };

        subscriptionConnectionsDetails.SubscriptionMode = IsConcurrent? "Concurrent" : "Single";
            
        foreach (var connection in _connections)
        {
            subscriptionConnectionsDetails.Results.Add(
                new SubscriptionConnectionDetails
                {
                    ClientUri = connection.ClientUri,
                    WorkerId = connection.WorkerId,
                    Strategy = connection.Strategy
                });
        }

        return subscriptionConnectionsDetails;
    }

    public IEnumerable<DocumentRecord> GetDocumentsFromResend(ClusterOperationContext context, HashSet<long> activeBatches)
    {
        var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
        using (GetDatabaseAndSubscriptionKeyPrefix(context, _databaseName, SubscriptionId, SubscriptionType.Document, out var prefix))
        using(Slice.External(context.Allocator, prefix, out var prefixSlice))
        {
            foreach (var (_, tvh) in subscriptionState.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
            {
                long batchId = Bits.SwapBytes(tvh.Reader.ReadLong((int)ClusterStateMachine.SubscriptionStateTable.BatchId));
                if (activeBatches.Contains(batchId))
                    continue;

                var id = tvh.Reader.ReadStringWithPrefix((int)ClusterStateMachine.SubscriptionStateTable.Key, prefix.Length);
                var cv = tvh.Reader.ReadString((int)ClusterStateMachine.SubscriptionStateTable.ChangeVector);
                    
                yield return new DocumentRecord
                {
                    DocumentId = id,
                    ChangeVector = cv
                };
            }
        }
    }

    public long GetNumberOfResendDocuments(SubscriptionType type)
    {
        using (_server.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
            using (GetDatabaseAndSubscriptionKeyPrefix(context, _databaseName, SubscriptionId, type, out var prefix))
            using (Slice.External(context.Allocator, prefix, out var prefixSlice))
            {
                return subscriptionState.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0).Count();
            }
        }
    }

    public IEnumerable<RevisionRecord> GetRevisionsFromResend(ClusterOperationContext context, HashSet<long> activeBatches)
    {
        var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
        using (GetDatabaseAndSubscriptionKeyPrefix(context, _databaseName, SubscriptionId, SubscriptionType.Revision, out var prefix))
        using (Slice.External(context.Allocator, prefix, out var prefixSlice))
        {
            foreach (var (_, tvh) in subscriptionState.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
            {
                var batchId = Bits.SwapBytes(tvh.Reader.ReadLong((int)ClusterStateMachine.SubscriptionStateTable.BatchId));
                if (activeBatches.Contains(batchId))
                    continue;

                string current = tvh.Reader.ReadStringWithPrefix((int)ClusterStateMachine.SubscriptionStateTable.Key, prefix.Length);
                string previous = tvh.Reader.ReadString((int)ClusterStateMachine.SubscriptionStateTable.ChangeVector);
                    
                yield return new RevisionRecord
                {
                    Current = current,
                    Previous = previous
                };
            }
        }
    }

    public bool IsDocumentInActiveBatch(ClusterOperationContext context, string documentId, HashSet<long> activeBatches)
    {
        var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
        using (GetDatabaseAndSubscriptionAndDocumentKey(context, _databaseName, SubscriptionId, documentId, out var key))
        using (Slice.External(context.Allocator,key, out var keySlice))
        {
            if (subscriptionState.ReadByKey(keySlice, out var reader) == false)
                return false;

            var batchId = Bits.SwapBytes(reader.ReadLong((int)ClusterStateMachine.SubscriptionStateTable.BatchId));
            return activeBatches.Contains(batchId);
        }
    }
        
    public bool IsRevisionInActiveBatch(ClusterOperationContext context, string current, HashSet<long> activeBatches)
    {
        var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
        using (GetDatabaseAndSubscriptionAndRevisionKey(context, _databaseName, SubscriptionId, current, out var key))
        using (Slice.External(context.Allocator,key, out var keySlice))
        {
            if (subscriptionState.ReadByKey(keySlice, out var reader) == false)
                return false;

            var batchId = Bits.SwapBytes(reader.ReadLong((int)ClusterStateMachine.SubscriptionStateTable.BatchId));
            return activeBatches.Contains(batchId);
        }
    }

    public Task AcknowledgeBatch(SubscriptionConnection connection, long batchId, List<DocumentRecord> addDocumentsToResend)
    {
        return AcknowledgeBatchProcessed(
            connection.LastSentChangeVectorInThisConnection ?? nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange),
            batchId,
            addDocumentsToResend);
    }

    protected virtual AcknowledgeSubscriptionBatchCommand GetAcknowledgeSubscriptionBatchCommand(string changeVector, long? batchId, List<DocumentRecord> docsToResend)
    {
        return new AcknowledgeSubscriptionBatchCommand(_databaseName, RaftIdGenerator.NewId())
        {
            ChangeVector = changeVector,
            NodeTag = _server.NodeTag,
            HasHighlyAvailableTasks = _server.LicenseManager.HasHighlyAvailableTasks(),
            SubscriptionId = SubscriptionId,
            SubscriptionName = SubscriptionName,
            LastTimeServerMadeProgressWithDocuments = DateTime.UtcNow,
            BatchId = batchId,
            DocumentsToResend = docsToResend,
        };
    }

    protected virtual UpdateSubscriptionClientConnectionTime GetUpdateSubscriptionClientConnectionTime()
    {
        return new UpdateSubscriptionClientConnectionTime(_databaseName, RaftIdGenerator.NewId())
        {
            NodeTag = _server.NodeTag,
            HasHighlyAvailableTasks = _server.LicenseManager.HasHighlyAvailableTasks(),
            SubscriptionName = SubscriptionName,
            LastClientConnectionTime = DateTime.UtcNow,
        };
    }


    public async Task AcknowledgeBatchProcessed(string changeVector, long? batchId, List<DocumentRecord> docsToResend)
    {
        var command = GetAcknowledgeSubscriptionBatchCommand(changeVector, batchId, docsToResend);
            
        var (etag, _) = await _server.SendToLeaderAsync(command);
        await WaitForIndexNotificationAsync(etag);
    }


    public async Task<long> RecordBatchRevisions(List<RevisionRecord> list, string lastRecordedChangeVector)
    {
        var command = new RecordBatchSubscriptionDocumentsCommand(
            _databaseName, 
            SubscriptionId, 
            SubscriptionName, 
            list, 
            PreviouslyRecordedChangeVector, 
            lastRecordedChangeVector, 
            _server.NodeTag, 
            _server.LicenseManager.HasHighlyAvailableTasks(), 
            RaftIdGenerator.NewId());

        return await RecordBatchInternal(command);
    }

    public async Task<long> RecordBatchDocuments(List<DocumentRecord> list, List<string> deleted, string lastRecordedChangeVector)
    {
        var command = new RecordBatchSubscriptionDocumentsCommand(
            _databaseName, 
            SubscriptionId, 
            SubscriptionName, 
            list, 
            PreviouslyRecordedChangeVector, 
            lastRecordedChangeVector, 
            _server.NodeTag, 
            _server.LicenseManager.HasHighlyAvailableTasks(), 
            RaftIdGenerator.NewId());

        command.Deleted = deleted;
        return await RecordBatchInternal(command);
    }

    protected virtual async Task<long> RecordBatchInternal(RecordBatchSubscriptionDocumentsCommand command)
    {
        var (etag, _) = await _server.SendToLeaderAsync(command);
        await WaitForIndexNotificationAsync(etag);
        return etag;
    }
    
    
    public bool IsSubscriptionActive()
    {
        return _connections.Count != 0;
    }

    public Task<bool> WaitForSubscriptionActiveLock(int millisecondsTimeout)
    {
        return _subscriptionActivelyWorkingLock.WaitAsync(millisecondsTimeout);
    }
        
    public void ReleaseSubscriptionActiveLock()
    {
        _subscriptionActivelyWorkingLock.Release();
    }

    public DisposeOnce<SingleAttempt> RegisterSubscriptionConnection(TSubscriptionConnection incomingConnection)
    {
        try
        {
            if (TryRegisterFirstConnection(incomingConnection)) 
                return GetDisposingAction();

            if (IsConcurrent)
            {
                if (incomingConnection.Strategy != SubscriptionOpeningStrategy.Concurrent)
                {
                    throw new SubscriptionInUseException(
                        $"Subscription {incomingConnection.Options.SubscriptionName} is currently concurrent and can only admit connections with a '{nameof(SubscriptionOpeningStrategy.Concurrent)}' {nameof(SubscriptionOpeningStrategy)}. Connection not opened.");
                }

                if (TryAddConnection(incomingConnection) == false)
                {
                    throw new InvalidOperationException("Could not add a connection to a concurrent subscription. Likely a bug");
                }
            }
            else
            {
                if (incomingConnection.Strategy == SubscriptionOpeningStrategy.Concurrent)
                {
                    throw new SubscriptionInUseException(
                        $"Subscription {incomingConnection.Options.SubscriptionName} is not concurrent and cannot admit connections with a '{nameof(SubscriptionOpeningStrategy.Concurrent)}' {nameof(SubscriptionOpeningStrategy)}. Connection not opened.");
                }

                RegisterSingleConnection(incomingConnection);
            }

            return GetDisposingAction();
        }
        catch (SubscriptionException e)
        {
            RegisterRejectedConnection(incomingConnection, e);
            throw;
        }

        DisposeOnce<SingleAttempt> GetDisposingAction()
        {
            return new DisposeOnce<SingleAttempt>(() =>
            {
                while (_recentConnections.Count > _maxConcurrentConnections + 10)
                {
                    _recentConnections.TryDequeue(out TSubscriptionConnection _);
                }
                _recentConnections.Enqueue(incomingConnection);
                DropSingleConnection(incomingConnection);
            });
        }
    }

    private bool TryRegisterFirstConnection(TSubscriptionConnection incomingConnection)
    {
        var current = _connections;
        if (current.Count == 0)
        {
            var firstConnection = new ConcurrentSet<TSubscriptionConnection> {incomingConnection};
            return Interlocked.CompareExchange(ref _connections, firstConnection, current) == current;
        }

        return false;
    }

    protected virtual void ValidateTakeOver(TSubscriptionConnection currentConnection)
    {
        if (currentConnection?.Strategy == SubscriptionOpeningStrategy.TakeOver)
            throw new SubscriptionInUseException(
                $"Subscription {currentConnection.Options.SubscriptionName} is already occupied by a TakeOver connection, connection cannot be opened");
    }

    private void RegisterSingleConnection(TSubscriptionConnection incomingConnection)
    {
        if (_connections.Count > 1)
        {
            throw new InvalidOperationException("Non concurrent subscription with more than a single connection. Likely a bug");
        }

        if (_connections.Count == 1)
        {
            var currentConnection = _connections.FirstOrDefault();
            switch (incomingConnection.Strategy)
            {
                case SubscriptionOpeningStrategy.OpenIfFree:
                    throw new SubscriptionInUseException(
                        $"Subscription {currentConnection?.Options.SubscriptionName} is occupied, connection cannot be opened");
                case SubscriptionOpeningStrategy.TakeOver:
                    
                    ValidateTakeOver(currentConnection);
                    
                    if (currentConnection != null)
                    {
                        DropSingleConnection(currentConnection, new SubscriptionInUseException("Closed by TakeOver"));
                    }

                    break;
                case SubscriptionOpeningStrategy.WaitForFree:
                    throw new TimeoutException();
                case SubscriptionOpeningStrategy.Concurrent:
                    throw new SubscriptionInUseException(
                        $"Subscription {currentConnection?.Options.SubscriptionName} does not accept concurrent connections at the moment, connection cannot be opened");
                default:
                    throw new InvalidOperationException("Unknown subscription open strategy: " +
                                                        incomingConnection.Strategy);
            }
        }

        RegisterOneConnection(incomingConnection);
    }

    private readonly MultipleUseFlag _addingSingleConnection = new MultipleUseFlag();

    private void RegisterOneConnection(TSubscriptionConnection incomingConnection)
    {
        if (_addingSingleConnection.Raise() == false)
            throw new TimeoutException();

        try
        {
            if (_connections.Count != 0)
                throw new TimeoutException();

            if (TryAddConnection(incomingConnection) == false)
            {
                throw new InvalidOperationException("Could not add a connection to a subscription. Likely a bug");
            }
        }
        finally
        {
            _addingSingleConnection.Lower();
        }
    }

    private bool TryAddConnection(TSubscriptionConnection connection)
    {
        int oldMax = _maxConcurrentConnections;
        if (_connections.TryAdd(connection) == false)  
            return false;
            
        var newMax = Math.Max(oldMax, _connections.Count);
        while (newMax > oldMax)
        {
            var replacedMax = Interlocked.CompareExchange(ref _maxConcurrentConnections, newMax, oldMax);
            if (replacedMax == oldMax)
                break;
            oldMax = replacedMax;
        }

        return true;
    }
    
    public void RegisterRejectedConnection(TSubscriptionConnection connection, SubscriptionException exception = null)
    {
        if (exception != null && connection.ConnectionException == null)
            connection.ConnectionException = exception;

        while (_rejectedConnections.Count > 10)
        {
            _rejectedConnections.TryDequeue(out TSubscriptionConnection _);
        }
        _rejectedConnections.Enqueue(connection);
    }

    private void CancelSingleConnection(TSubscriptionConnection connection, SubscriptionException ex)
    {
        try
        {
            connection.ConnectionException = ex;
            connection.CancellationTokenSource.Cancel();
            RegisterRejectedConnection(connection, ex);
        }
        catch
        {
            //
        }
    }

    public void DropSingleConnection(TSubscriptionConnection connection, SubscriptionException ex = null)
    {
        if (_connections.TryRemove(connection))
        {
            CancelSingleConnection(connection, ex ?? connection.ConnectionException ?? new SubscriptionClosedException($"Connection {connection.ClientUri} has closed", canReconnect: true));
            NotifyHasMoreDocs(); //upon connection fail, all recorded docs should be resent
        }
    }

    public TSubscriptionConnection MostRecentEndedConnection()
    {
        if (_recentConnections.TryPeek(out var recentConnection))
            return recentConnection;
        return null;
    }

    public List<TSubscriptionConnection> GetConnections()
    {
        return _connections.ToList();
    }


    public string GetConnectionsAsString()
    {
        StringBuilder sb = null;
        foreach (var connection in _connections)
        {
            sb ??= new StringBuilder();
            sb.AppendLine($"{connection.ClientUri}");
        }
        return sb?.ToString();
    }

    public override string ToString()
    {
        return $"{nameof(_subscriptionId)}: {_subscriptionId}";
    }

    public void EndConnections()
    {
        foreach (var connection in _connections)
        {
            DropSingleConnection(connection);
        }
    }
        
    public virtual void Dispose()
    {
        try
        {
            CancellationTokenSource.Cancel();
        }
        catch
        {
            // ignored: If we've failed to raise the cancellation token, it means that it's already raised
        }

        try
        {
            EndConnections();
        }
        catch
        {
            // ignored: If we've failed to raise the cancellation token, it means that it's already raised
        }
    }

    private static ByteStringContext<ByteStringMemoryCache>.InternalScope GetDatabaseAndSubscriptionKeyPrefix(ClusterOperationContext context, string database, long subscriptionId, SubscriptionType type, out ByteString prefix)
    {
        using var _ = Slice.From(context.Allocator, database.ToLowerInvariant(), out var dbName);
        var rc = context.Allocator.Allocate(dbName.Size + sizeof(byte) + sizeof(long) + sizeof(byte) + sizeof(byte) + sizeof(byte), out prefix);

        PopulatePrefix(subscriptionId, type, ref prefix, ref dbName, out var __);

        return rc;
    }

    public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetDatabaseAndSubscriptionAndDocumentKey(ClusterOperationContext context, string database, long subscriptionId, string documentId, out ByteString key)
    {
        return GetSubscriptionStateKey(context, database, subscriptionId, documentId, SubscriptionType.Document, out key);
    }

    public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetDatabaseAndSubscriptionAndRevisionKey(ClusterOperationContext context, string database, long subscriptionId, string currentChangeVector, out ByteString key)
    {
        return GetSubscriptionStateKey(context, database, subscriptionId, currentChangeVector, SubscriptionType.Revision, out key);
    }

    public static unsafe ByteStringContext<ByteStringMemoryCache>.InternalScope GetDatabaseAndSubscriptionPrefix(ClusterOperationContext context, string database, long subscriptionId, out ByteString prefix)
    {
        using var _ = Slice.From(context.Allocator, database.ToLowerInvariant(), out var dbName);
        var rc = context.Allocator.Allocate(dbName.Size + sizeof(byte) + sizeof(long) + sizeof(byte), out prefix);

        dbName.CopyTo(prefix.Ptr);
        var position = dbName.Size;

        *(prefix.Ptr + position) = SpecialChars.RecordSeparator;
        position++;

        *(long*)(prefix.Ptr + position) = subscriptionId;
        position += sizeof(long);

        *(prefix.Ptr + position) = SpecialChars.RecordSeparator;

        return rc;
    }

    public static unsafe ByteStringContext<ByteStringMemoryCache>.InternalScope GetSubscriptionStateKey(ClusterOperationContext context, string database, long subscriptionId, string pk, SubscriptionType type, out ByteString key)
    {
        switch (type)
        {
            case SubscriptionType.Document:
                pk = pk.ToLowerInvariant();
                break;
            case SubscriptionType.Revision:
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(type), type, null);
        }

        using var _ = Slice.From(context.Allocator, database.ToLowerInvariant(), out var dbName);
        using var __ = Slice.From(context.Allocator, pk, out var pkSlice);
        var rc = context.Allocator.Allocate(dbName.Size + sizeof(byte) + sizeof(long) + sizeof(byte) + sizeof(byte) + sizeof(byte) + pkSlice.Size, out key);

        PopulatePrefix(subscriptionId, type, ref key, ref dbName, out int position);

        pkSlice.CopyTo(key.Ptr + position);
        return rc;
    }
        
    private static unsafe void PopulatePrefix(long subscriptionId, SubscriptionType type, ref ByteString prefix, ref Slice dbName, out int position)
    {
        dbName.CopyTo(prefix.Ptr);
        position = dbName.Size;

        *(prefix.Ptr + position) = SpecialChars.RecordSeparator;
        position++;

        *(long*)(prefix.Ptr + position) = subscriptionId;
        position += sizeof(long);

        *(prefix.Ptr + position) = SpecialChars.RecordSeparator;
        position++;

        *(prefix.Ptr + position) = (byte)type;
        position++;

        *(prefix.Ptr + position) = SpecialChars.RecordSeparator;
        position++;
    }
}
