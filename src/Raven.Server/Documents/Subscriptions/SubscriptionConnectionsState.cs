using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Collections;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Voron;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionConnectionsState : IDisposable
    {
        private readonly long _subscriptionId;
        private readonly SubscriptionStorage _subscriptionStorage;
        private readonly AsyncManualResetEvent _waitForMoreDocuments;
        private DocumentsStorage _documentsStorage;
        private SubscriptionState _subscriptionState;
        private ConcurrentSet<SubscriptionConnection> _connections;
        private string _subscriptionName;
        private int _maxConcurrentConnections;

        public string Query;
        public Task GetSubscriptionInUseAwaiter => Task.WhenAll(_connections.Select(c => c.SubscriptionConnectionTask));
        public string SubscriptionName => _subscriptionName;
        public long SubscriptionId => _subscriptionId;

        public bool IsConcurrent => _connections.FirstOrDefault()?.Strategy == SubscriptionOpeningStrategy.Concurrent;

        internal ConcurrentSet<SubscriptionConnectionInfo> _pendingConnections = new ();
        private readonly ConcurrentQueue<SubscriptionConnectionInfo> _recentConnections = new ();
        private readonly ConcurrentQueue<SubscriptionConnectionInfo> _rejectedConnections = new ();
        public IEnumerable<SubscriptionConnectionInfo> RecentConnections => _recentConnections;
        public IEnumerable<SubscriptionConnectionInfo> RecentRejectedConnections => _rejectedConnections;
        public IEnumerable<SubscriptionConnectionInfo> PendingConnections => _pendingConnections;

        public CancellationTokenSource CancellationTokenSource;

        public DocumentDatabase DocumentDatabase => _documentsStorage.DocumentDatabase;

        private readonly SemaphoreSlim _subscriptionActivelyWorkingLock;
        private readonly SemaphoreSlim _subscriptionConnectingLock = new SemaphoreSlim(1);

        public string LastChangeVectorSent;

        public string PreviouslyRecordedChangeVector;
        private IDisposable _disposableNotificationsRegistration;

        public SubscriptionConnectionsState(long subscriptionId, SubscriptionStorage storage)
        {
            _subscriptionId = subscriptionId;
            _connections = new ConcurrentSet<SubscriptionConnection>();
            _subscriptionStorage = storage;
            _documentsStorage = storage._db.DocumentsStorage;
            _subscriptionActivelyWorkingLock = new SemaphoreSlim(1);
            CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(storage._db.DatabaseShutdown);
            _waitForMoreDocuments = new AsyncManualResetEvent(CancellationTokenSource.Token);
        }

        public async Task InitializeAsync(SubscriptionConnection connection, bool afterSubscribe = false)
        {
            _subscriptionName = connection.Options.SubscriptionName ?? _subscriptionId.ToString();
            Query = connection.SubscriptionState.Query;

            if (afterSubscribe == false)
                return;

            // update the subscription data only on new concurrent connection or regular connection
            if (IsConcurrent == false)
            {
                RefreshFeatures(connection);
                return;
            }

            connection.AddToStatusDescription("Starting to subscribe.");

            if (_subscriptionState == null)
            {
                // this connection is the first one, we initialize everything
                RefreshFeatures(connection);
                return;
            }

            if (connection.SubscriptionState.RaftCommandIndex < _subscriptionState.RaftCommandIndex)
            {
                // this connection was modified while waiting to subscribe, lets try to drop it
                DropSingleConnection(connection, new SubscriptionClosedException($"The subscription '{_subscriptionName}' was modified, connection have to be restarted.", canReconnect: true));
                return;
            }

            if (connection.SubscriptionState.RaftCommandIndex == _subscriptionState.RaftCommandIndex)
            {
                // no changes in the subscription task 
                // the LastChangeVectorSent have to be refreshed since the concurrent subscription task might got processed on different node
                if (_connections.Count == 1)
                {
                    InitializeLastChangeVectorSent(connection.SubscriptionState.ChangeVectorForNextBatchStartingPoint);
                }

                return;
            }

            if (connection.SubscriptionState.RaftCommandIndex > _subscriptionState.RaftCommandIndex)
            {
                // we have new connection after subscription have changed
                // we have to wait until old connections (with smaller raft index) will get disconnected
                // then we continue and will re-initialize 

                var sp = Stopwatch.StartNew();
                while (_connections.Any(c => c.SubscriptionState.RaftCommandIndex == _subscriptionState.RaftCommandIndex))
                {
                    connection.CancellationTokenSource.Token.ThrowIfCancellationRequested();
                    await Task.Delay(300);
                    await connection.SendHeartBeatIfNeededAsync(sp, $"A connection from IP '{connection.ClientUri}' is waiting for old subscription connections to disconnect.");
                }

                RefreshFeatures(connection);
            }
        }

        public void ReleaseConcurrentConnectionLock(SubscriptionConnection connection)
        {
            if (connection.Strategy != SubscriptionOpeningStrategy.Concurrent)
                return;

            _subscriptionConnectingLock.Release();
        }

        public async Task TakeConcurrentConnectionLockAsync(SubscriptionConnection connection)
        {
            if (connection.Strategy != SubscriptionOpeningStrategy.Concurrent)
                return;

            DocumentDatabase.ForTestingPurposes?.ConcurrentSubscription_ActionToCallDuringWaitForSubscribe?.Invoke(_connections);

            while (await _subscriptionConnectingLock.WaitAsync(SubscriptionConnection.WaitForChangedDocumentsTimeoutInMs) == false)
            {
                connection.CancellationTokenSource.Token.ThrowIfCancellationRequested();
                await connection.SendHeartBeatAsync($"A connection from IP '{connection.ClientUri}' is waiting for other concurrent connections to subscribe.");
            }
        }

        private void RefreshFeatures(SubscriptionConnection connection)
        {
            using (var old = _disposableNotificationsRegistration)
            {
                _disposableNotificationsRegistration = RegisterForNotificationOnNewDocuments(connection);
            }

            InitializeLastChangeVectorSent(connection.SubscriptionState.ChangeVectorForNextBatchStartingPoint);

            _subscriptionState = connection.SubscriptionState;
        }

        internal void InitializeLastChangeVectorSent(string changeVectorForNextBatchStartingPoint)
        {
            LastChangeVectorSent = changeVectorForNextBatchStartingPoint;
            PreviouslyRecordedChangeVector = LastChangeVectorSent;
        }

        public IEnumerable<DocumentRecord> GetDocumentsFromResend(ClusterOperationContext context, HashSet<long> activeBatches)
        {
            var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
            using (GetDatabaseAndSubscriptionKeyPrefix(context, DocumentDatabase.Name, SubscriptionId, SubscriptionType.Document, out var prefix))
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
            using (DocumentDatabase.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
                using (GetDatabaseAndSubscriptionKeyPrefix(context, DocumentDatabase.Name, SubscriptionId, type, out var prefix))
                using (Slice.External(context.Allocator, prefix, out var prefixSlice))
                {
                    return subscriptionState.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0).Count();
                }
            }
        }

        public IEnumerable<RevisionRecord> GetRevisionsFromResend(ClusterOperationContext context, HashSet<long> activeBatches)
        {
            var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
            using (GetDatabaseAndSubscriptionKeyPrefix(context, DocumentDatabase.Name, SubscriptionId, SubscriptionType.Revision, out var prefix))
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
            using (GetDatabaseAndSubscriptionAndDocumentKey(context, DocumentDatabase.Name, SubscriptionId, documentId, out var key))
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
            using (GetDatabaseAndSubscriptionAndRevisionKey(context, DocumentDatabase.Name, SubscriptionId, current, out var key))
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
            return connection.TcpConnection.DocumentDatabase.SubscriptionStorage.AcknowledgeBatchProcessed(
                SubscriptionId,
                SubscriptionName,
                connection.LastSentChangeVectorInThisConnection ?? nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange),
                batchId,
                addDocumentsToResend);
        }
        
        public long GetLastEtagSent()
        {
            return ChangeVectorUtils.GetEtagById(LastChangeVectorSent, _documentsStorage.DocumentDatabase.DbBase64Id);
        }

        public bool IsSubscriptionActive()
        {
            return _connections.IsEmpty == false;
        }

        public Task<bool> WaitForSubscriptionActiveLock(int millisecondsTimeout)
        {
            return _subscriptionActivelyWorkingLock.WaitAsync(millisecondsTimeout);
        }
        
        public void ReleaseSubscriptionActiveLock()
        {
            _subscriptionActivelyWorkingLock.Release();
        }

        public DisposeOnce<SingleAttempt> RegisterSubscriptionConnection(SubscriptionConnection incomingConnection)
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
                        _recentConnections.TryDequeue(out SubscriptionConnectionInfo _);
                    }

                    _recentConnections.Enqueue(new SubscriptionConnectionInfo(incomingConnection));

                    DropSingleConnection(incomingConnection);
                });
            }
        }

        private bool TryRegisterFirstConnection(SubscriptionConnection incomingConnection)
        {
            var current = _connections;
            if (current.IsEmpty)
            {
                var firstConnection = new ConcurrentSet<SubscriptionConnection> {incomingConnection};
                return Interlocked.CompareExchange(ref _connections, firstConnection, current) == current;
            }

            return false;
        }


        private void RegisterSingleConnection(SubscriptionConnection incomingConnection)
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
                        if (currentConnection?.Strategy == SubscriptionOpeningStrategy.TakeOver)
                            throw new SubscriptionInUseException(
                                $"Subscription {currentConnection.Options.SubscriptionName} is already occupied by a TakeOver connection, connection cannot be opened");

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

        private void RegisterOneConnection(SubscriptionConnection incomingConnection)
        {
            if (_addingSingleConnection.Raise() == false)
                throw new TimeoutException();

            try
            {
                if (_connections.IsEmpty == false)
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

        private bool TryAddConnection(SubscriptionConnection connection)
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

        public void DropSubscription(SubscriptionException e)
        {
            _subscriptionStorage.DropSubscriptionConnections(SubscriptionId, e);
        }

        public void RegisterRejectedConnection(SubscriptionConnection connection, SubscriptionException exception = null)
        {
            if (exception != null && connection.ConnectionException == null)
                connection.ConnectionException = exception;

            while (_rejectedConnections.Count > 10)
            {
                _rejectedConnections.TryDequeue(out SubscriptionConnectionInfo _);
            }

            _rejectedConnections.Enqueue(new SubscriptionConnectionInfo(connection));
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

        private void CancelSingleConnection(SubscriptionConnection connection, SubscriptionException ex)
        {
            try
            {
                connection.ConnectionException = ex;
                connection.CancellationTokenSource.Cancel();
            }
            catch
            {
                //
            }
        }

        public void DropSingleConnection(SubscriptionConnection connection, SubscriptionException ex = null)
        {
            if (_connections.TryRemove(connection))
            {
                CancelSingleConnection(connection, ex ?? connection.ConnectionException ?? new SubscriptionClosedException($"Connection {connection.ClientUri} has closed", canReconnect: true));
                NotifyHasMoreDocs(); //upon connection fail, all recorded docs should be resent
            }
        }

        public IDisposable RegisterForNotificationOnNewDocuments(SubscriptionConnection connection)
        {
            void RegisterNotification(DocumentChange notification)
            {
                if (Client.Constants.Documents.Collections.AllDocumentsCollection.Equals(connection.Subscription.Collection, StringComparison.OrdinalIgnoreCase) ||
                    notification.CollectionName.Equals(connection.Subscription.Collection, StringComparison.OrdinalIgnoreCase))
                {
                    _waitForMoreDocuments.Set();
                }
            }

            DocumentDatabase.Changes.OnDocumentChange += RegisterNotification;

            return new DisposableAction(
                () =>
                {
                    DocumentDatabase.Changes.OnDocumentChange -= RegisterNotification;
                });
        }

        public SubscriptionConnectionInfo MostRecentEndedConnection()
        {
            if (_recentConnections.TryPeek(out var recentConnection))
                return recentConnection;
            return null;
        }

        public void NotifyHasMoreDocs()
        {
            _waitForMoreDocuments.Set();
        }

        public void NotifyNoMoreDocs()
        {
            _waitForMoreDocuments.Reset();
        }

        public Task<bool> WaitForMoreDocs()
        {
            return _waitForMoreDocuments.WaitAsync();
        }

        public List<SubscriptionConnection> GetConnections()
        {
            return _connections.ToList();
        }

        public HashSet<long> GetActiveBatches()
        {
            var set = new HashSet<long>();
            
            foreach (var connection in _connections)
            {
                var batch = connection.CurrentBatchId;
                if (batch == SubscriptionConnection.NonExistentBatch)
                    continue;

                set.Add(batch);
            }

            return set;
        }

        public string GetConnectionsAsString()
        {
            StringBuilder sb = null;
            foreach (var connection in _connections)
            {
                sb ??= new StringBuilder();
                sb.AppendLine($"{connection.TcpConnection.TcpClient.Client.RemoteEndPoint}");
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
        
        public void Dispose()
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

            _disposableNotificationsRegistration?.Dispose();
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

        // dummy state
        private SubscriptionConnectionsState(DocumentsStorage storage, SubscriptionState state)
        {
            //the first connection to join creates this object and decides all details of the subscription
            _subscriptionName = "dummy";
            _subscriptionId = -0x42;
            _documentsStorage = storage;
            Query = state.Query;

            _subscriptionStorage = storage.DocumentDatabase.SubscriptionStorage;

            CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_documentsStorage.DocumentDatabase.DatabaseShutdown);
            _waitForMoreDocuments = new AsyncManualResetEvent(CancellationTokenSource.Token);

            _subscriptionActivelyWorkingLock = new SemaphoreSlim(1);
            LastChangeVectorSent = state.ChangeVectorForNextBatchStartingPoint;
            PreviouslyRecordedChangeVector = LastChangeVectorSent;
        }

        public static SubscriptionConnectionsState CreateDummyState(DocumentsStorage storage, SubscriptionState state) => new(storage, state);

        private long _lastNoopAckTicks;
        private Task _lastNoopAckTask = Task.CompletedTask;

        public Task SendNoopAck()
        {
            if (ClusterCommandsVersionManager.CurrentClusterMinimalVersion >= 53_000)
            {
                if (IsConcurrent)
                {
                    var localLastNoopAckTicks = Interlocked.Read(ref _lastNoopAckTicks);
                    var nowTicks = DateTime.Now.Ticks;
                    if ((nowTicks - localLastNoopAckTicks) / TimeSpan.TicksPerMillisecond < SubscriptionConnection.WaitForChangedDocumentsTimeoutInMs)
                    {
                        return _lastNoopAckTask;
                    }

                    var currentLastNoopAckTicks = Interlocked.CompareExchange(ref _lastNoopAckTicks, nowTicks, localLastNoopAckTicks);
                    if (currentLastNoopAckTicks != localLastNoopAckTicks)
                        return _lastNoopAckTask;

                    var ackTask = SendNoopAckAsync(SubscriptionId, SubscriptionName);

                    Interlocked.Exchange(ref _lastNoopAckTask, ackTask);

                    return ackTask;
                }

                return SendNoopAckAsync(SubscriptionId, SubscriptionName);
            }

            return DocumentDatabase.SubscriptionStorage.LegacyAcknowledgeBatchProcessed(
                SubscriptionId,
                SubscriptionName,
                nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange),
                nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange));
        }

        private async Task SendNoopAckAsync(long subscriptionId, string name)
        {
            var command = new AcknowledgeSubscriptionBatchCommand(_documentsStorage.DocumentDatabase.Name, RaftIdGenerator.NewId())
            {
                ChangeVector = nameof(Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange),
                NodeTag = _documentsStorage.DocumentDatabase.ServerStore.NodeTag,
                HasHighlyAvailableTasks = _documentsStorage.DocumentDatabase.ServerStore.LicenseManager.HasHighlyAvailableTasks(),
                SubscriptionId = subscriptionId,
                SubscriptionName = name,
                BatchId = SubscriptionConnection.NonExistentBatch,
                LastTimeServerMadeProgressWithDocuments = DateTime.UtcNow,
                DatabaseName = _documentsStorage.DocumentDatabase.Name,
            };

            var state = _documentsStorage.DocumentDatabase.ServerStore.Engine.CurrentState;
            if (state == RachisState.Leader || state == RachisState.Follower)
            {
                // there are no changes for this subscription but we still want to check if we are the node that is responsible for this task.
                // we can do that locally if we have a functional cluster (in a leader or follower state).

                using (_documentsStorage.DocumentDatabase.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var rawDatabaseRecord = _documentsStorage.DocumentDatabase.ServerStore.Cluster.ReadRawDatabaseRecord(context, command.DatabaseName);
                    if (rawDatabaseRecord == null)
                        throw new DatabaseDoesNotExistException($"Cannot set command value of type {nameof(AcknowledgeSubscriptionBatchCommand)} for database {command.DatabaseName}, because it does not exist");

                    var subscription = _documentsStorage.DocumentDatabase.ServerStore.Cluster.Read(context, command.GetItemId());
                    if (subscription == null)
                        throw new SubscriptionDoesNotExistException($"Subscription with name '{command.SubscriptionName}' does not exist");

                    var subscriptionTask = new AcknowledgeSubscriptionBatchCommand.SubscriptionTask(subscription);
                    command.AssertSubscriptionState(rawDatabaseRecord, subscriptionTask, command.SubscriptionName);
                    return;
                }
            }

            if (_subscriptionStorage.ShouldWaitForClusterStabilization())
            {
                // in case of unstable cluster, we will try to heartbeat the client for 30 seconds and then send actual no-op command
                return;
            }

            var (etag, _) = await _documentsStorage.DocumentDatabase.ServerStore.SendToLeaderAsync(command);
            await _documentsStorage.DocumentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(etag, _documentsStorage.DocumentDatabase.ServerStore.Engine.OperationTimeout);
        }
    }
}
