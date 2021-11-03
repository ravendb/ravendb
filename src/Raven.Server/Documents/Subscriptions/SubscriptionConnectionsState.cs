using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Server;
using Sparrow.Threading;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionConnectionsState
    {
        private readonly string _subscriptionName;
        private readonly long _subscriptionId;
        private readonly SubscriptionStorage _subscriptionStorage;
        private readonly DocumentsStorage _documentsStorage;
        private readonly SubscriptionState _subscriptionState;
        private ConcurrentSet<SubscriptionConnection> _connections;
        private int _maxConcurrentConnections;

        private AsyncManualResetEvent _waitForMoreDocuments;

        public readonly string Query;
        public string SubscriptionName => _subscriptionName;
        public long SubscriptionId => _subscriptionId;
        public SubscriptionState SubscriptionState => _subscriptionState;

        public readonly bool IsConcurrent;

        private readonly ConcurrentSet<SubscriptionConnection> _pendingConnections = new ();
        private readonly ConcurrentQueue<SubscriptionConnection> _recentConnections = new ();
        private readonly ConcurrentQueue<SubscriptionConnection> _rejectedConnections = new ();
        public IEnumerable<SubscriptionConnection> RecentConnections => _recentConnections;
        public IEnumerable<SubscriptionConnection> RecentRejectedConnections => _rejectedConnections;
        public ConcurrentSet<SubscriptionConnection> PendingConnections => _pendingConnections;

        public CancellationTokenSource CancellationTokenSource;

        public DocumentDatabase DocumentDatabase => _documentsStorage.DocumentDatabase;

        private readonly SemaphoreSlim _subscriptionActivelyWorkingLock;

        public string LastChangeVectorSent;
        
        public string PreviouslyRecordedChangeVector;
        public SubscriptionConnectionsState(string subscriptionName, long subscriptionId, SubscriptionStorage storage, SubscriptionConnection connection)
        {
            //the first connection to join creates this object and decides all details of the subscription
            _subscriptionName = subscriptionName ?? subscriptionId.ToString();
            _subscriptionId = subscriptionId;
            _subscriptionState = connection.SubscriptionState;
            _documentsStorage = connection.TcpConnection.DocumentDatabase.DocumentsStorage;
            Query = connection.SubscriptionState.Query;
            _connections = new ConcurrentSet<SubscriptionConnection>();
            IsConcurrent = connection.Strategy == SubscriptionOpeningStrategy.Concurrent;
            
            _subscriptionStorage = storage;

            CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_documentsStorage.DocumentDatabase.DatabaseShutdown);
            _waitForMoreDocuments = new AsyncManualResetEvent(CancellationTokenSource.Token);
            var disposableNotificationsRegistration = RegisterForNotificationOnNewDocuments(connection); //TODO stav: dispose of this

            _subscriptionActivelyWorkingLock = new SemaphoreSlim(1);
            LastChangeVectorSent = connection.SubscriptionState.ChangeVectorForNextBatchStartingPoint;
            PreviouslyRecordedChangeVector = LastChangeVectorSent;
            Console.WriteLine($"Creating concurrent subscription. Starting from cv {LastChangeVectorSent}");
        }

        public IEnumerable<(Document Doc, Exception Exception)> GetNextBatch(SubscriptionConnection connection, SubscriptionDocumentsFetcher fetcher,
            DocumentsOperationContext docsContext, IncludeDocumentsCommand includesCmd)
        {
            int docsAmount = 0;
            var clusterStateMachine = _documentsStorage.DocumentDatabase.ServerStore.Cluster;
            using (_documentsStorage.DocumentDatabase.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext clusterOperationContext))
            using (clusterOperationContext.OpenReadTransaction())
            {
                foreach ((Document doc, Exception ex) in fetcher.GetDataToSend(clusterOperationContext, docsContext, includesCmd, GetLastEtagSent()))
                {
                    if (docsAmount >= connection.Options.MaxDocsPerBatch)
                    {
                        yield break;
                    }
                    
                    if (clusterStateMachine.IsDocumentInActiveBatch(clusterOperationContext, connection.TcpConnection.DocumentDatabase.Name, SubscriptionId, doc.Id,
                        _connections.Select(conn => conn.CurrentBatchId).ToHashSet()))
                    {
                        continue;
                    }

                    docsAmount++;
                    yield return (doc, ex);
                }
            }
        }

        public IEnumerable<DocumentRecord> GetDocumentsFromResend(ClusterOperationContext clusterOperationContext)
        {
            var clusterStateMachine = _documentsStorage.DocumentDatabase.ServerStore.Cluster;
            return clusterStateMachine.GetDocumentsFromResend(clusterOperationContext,
                DocumentDatabase.Name, SubscriptionId, _connections.Select(conn => conn.CurrentBatchId).ToHashSet());
        }

        public IEnumerable<RevisionRecord> GetRevisionsFromResend(ClusterOperationContext clusterOperationContext)
        {
            var clusterStateMachine = _documentsStorage.DocumentDatabase.ServerStore.Cluster;
            return clusterStateMachine.GetRevisionsFromResend(clusterOperationContext,
                DocumentDatabase.Name, SubscriptionId, _connections.Select(conn => conn.CurrentBatchId).ToHashSet());
        }

        public Task AcknowledgeBatch(SubscriptionConnection connection, long batchId, HashSet<string> addDocumentsToResend)
        {
            Task ackTask;
            lock (this)
            {
                ackTask = connection.TcpConnection.DocumentDatabase.SubscriptionStorage.AcknowledgeBatchProcessed(
                    SubscriptionId,
                    SubscriptionName,
                    connection.LastSentChangeVectorInThisConnection ?? nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange),
                    "",
                    batchId,
                    addDocumentsToResend);
            }
            return ackTask;
        }
        
        private long GetLastEtagSent()
        {
            return ChangeVectorUtils.GetEtagById(LastChangeVectorSent, _documentsStorage.DocumentDatabase.DbBase64Id);
        }

        public bool IsSubscriptionActive()
        {
            return _connections.Count != 0;
        }
        
        public enum DocumentState
        {
            Updated,
            Unchanged,
            Conflicted,
            Deleted
        }
        
        

        internal Document GetRevision(string changeVector,DocumentsOperationContext context)
        {
            return DocumentDatabase.DocumentsStorage.RevisionsStorage.GetRevision(context, changeVector);
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
                if (IsConcurrent)
                {
                    if(incomingConnection.Strategy != SubscriptionOpeningStrategy.Concurrent)
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
                        _recentConnections.TryDequeue(out SubscriptionConnection _);
                    }
                    _recentConnections.Enqueue(incomingConnection);
                    DropSingleConnection(incomingConnection);
                });
            }
        }

        private void RegisterSingleConnection(SubscriptionConnection incomingConnection)
        {
            lock (this)
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
                                _connections.TryRemove(currentConnection);
                                CancelSingleConnection(currentConnection, new SubscriptionInUseException("Closed by TakeOver"));
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
                Console.WriteLine($"Registered connection {incomingConnection.TcpConnection.TcpClient.Client.RemoteEndPoint}");
                if (TryAddConnection(incomingConnection) == false)
                {
                    throw new InvalidOperationException("Could not add a connection to a subscription. Likely a bug");
                }
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

        public bool TryRemoveConnection(SubscriptionConnection connection)
        {
            return _connections.TryRemove(connection);
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
                _rejectedConnections.TryDequeue(out SubscriptionConnection _);
            }
            _rejectedConnections.Enqueue(connection);
        }

        public SubscriptionConnectionsDetails GetSubscriptionConnectionsDetails()
        {
            var subscriptionConnectionsDetails = new SubscriptionConnectionsDetails
            {
                Details = new List<SubscriptionConnectionDetails>()
            };

            foreach (var connection in _connections)
            {
                subscriptionConnectionsDetails.Details.Add(
                    new SubscriptionConnectionDetails
                    {
                        ClientUri = connection.ClientUri,
                        Strategy = connection.Strategy
                    });
            }

            return subscriptionConnectionsDetails;
        }

        public void CancelSingleConnection(SubscriptionConnection connection, SubscriptionException ex)
        {
            RegisterRejectedConnection(connection, ex);
            connection.ConnectionException = ex;
            try
            {
                connection.CancellationTokenSource.Cancel();
            }
            catch
            {
                //
            }
        }

        public void DropSingleConnection(SubscriptionConnection connection)
        {
            if (_connections.Contains(connection))
            {
                Console.WriteLine("\nDropping connection\n");
                try
                {
                    _connections.TryRemove(connection);
                }
                finally
                {
                    NotifyHasMoreDocs(); //upon connection fail, all recorded docs should be resent
                }
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

        public void DropCancelledConnections()
        {
            foreach (var connection in _connections)
            {
                if(connection.CancellationTokenSource.IsCancellationRequested)
                    DropSingleConnection(connection);
            }
        }

        public SubscriptionConnection MostRecentEndedConnection()
        {
            if (_recentConnections.TryPeek(out var recentConnection))
                return recentConnection;
            return null;
        }

        public void NotifyHasMoreDocs()
        {
            Console.WriteLine("Has More Docs");
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

        public override string ToString()
        {
            return $"{nameof(_subscriptionId)}: {_subscriptionId}";
        }

        public void EndConnections()
        {
            foreach (var conn in _connections)
            {
                try
                {
                    conn.CancellationTokenSource.Cancel();
                }
                catch
                {
                    // ignored: If we've failed to raise the cancellation token, it means that it's already raised
                }
            }
        }

        public void Dispose()
        {
            try
            {
                CancellationTokenSource.Cancel();
                EndConnections();
            }
            catch
            {
                // ignored: If we've failed to raise the cancellation token, it means that it's already raised
            }
        }
    }
}
