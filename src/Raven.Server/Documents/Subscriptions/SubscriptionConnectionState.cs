using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.Sharding.Documents;
using Sparrow.Collections;
using Sparrow.Server;
using Sparrow.Threading;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionConnectionState : IDisposable
    {
        private readonly long _subscriptionId;
        private readonly SubscriptionStorage _storage;
        internal readonly AsyncManualResetEvent ConnectionInUse = new AsyncManualResetEvent();

        private readonly ShardedSubscriptionContext _shardedContext;
        private readonly bool _isSharded;

        public SubscriptionConnectionState(long subscriptionId, string subscriptionName, SubscriptionStorage storage)
        {
            _subscriptionId = subscriptionId;
            SubscriptionName = subscriptionName??subscriptionId.ToString();
            _storage = storage;
            ConnectionInUse.Set();
        }

        public SubscriptionConnectionState(long subscriptionId, string subscriptionName, ShardedSubscriptionContext context)
        {
            _subscriptionId = subscriptionId;
            SubscriptionName = subscriptionName ?? subscriptionId.ToString();
            _shardedContext = context;
            _isSharded = true;
            ConnectionInUse.Set();
        }

        public override string ToString()
        {
            return $"{nameof(_subscriptionId)}: {_subscriptionId}";
        }

        private SubscriptionConnectionBase _currentConnection;

        private readonly ConcurrentSet<SubscriptionConnectionBase> _pendingConnections = new ConcurrentSet<SubscriptionConnectionBase>();
        private readonly ConcurrentQueue<SubscriptionConnectionBase> _recentConnections = new ConcurrentQueue<SubscriptionConnectionBase>();
        private readonly ConcurrentQueue<SubscriptionConnectionBase> _rejectedConnections = new ConcurrentQueue<SubscriptionConnectionBase>();

        public SubscriptionConnectionBase Connection => _currentConnection;

        // we should have two locks: one lock for a connection and one lock for operations
        // remember to catch ArgumentOutOfRangeException for timeout problems
        public async Task<DisposeOnce<SingleAttempt>> RegisterSubscriptionConnection(
            SubscriptionConnectionBase incomingConnection,
            TimeSpan timeToWait)
        {
            try
            {
                if (await ConnectionInUse.WaitAsync(timeToWait) == false)
                {
                    switch (incomingConnection.Strategy)
                    {
                        // we try to connect, if the resource is occupied, we will throw an exception
                        // this piece of code could have been upper, but we choose to have it here, for better readability
                        case SubscriptionOpeningStrategy.WaitForFree:
                            throw new TimeoutException();

                        case SubscriptionOpeningStrategy.OpenIfFree:
                            throw new SubscriptionInUseException(
                                $"Subscription {incomingConnection.Options.SubscriptionName} is occupied, connection cannot be opened");

                        case SubscriptionOpeningStrategy.TakeOver:
                            if (_currentConnection?.Strategy == SubscriptionOpeningStrategy.TakeOver)
                                throw new SubscriptionInUseException(
                                    $"Subscription {incomingConnection.Options.SubscriptionName} is already occupied by a TakeOver connection, connection cannot be opened");

                            if (_currentConnection != null)
                            {
                                if (_isSharded)
                                {
                                   await _shardedContext.DropSubscriptionConnectionAndPropagateToShards(_currentConnection.SubscriptionId, new SubscriptionInUseException("Closed by TakeOver"));
                                }
                                else
                                {
                                    _storage.DropSubscriptionConnection(_currentConnection.SubscriptionId, new SubscriptionInUseException("Closed by TakeOver"));
                                }
                            }

                            throw new TimeoutException();

                        default:
                            throw new InvalidOperationException("Unknown subscription open strategy: " +
                                                                incomingConnection.Strategy);
                    }
                }
            }
            catch (SubscriptionException e)
            {
                RegisterRejectedConnection(incomingConnection, e);
                throw;
            }

            var subscriptionConnection = Interlocked.CompareExchange(ref _currentConnection, incomingConnection, null);
            if (subscriptionConnection != null && subscriptionConnection != incomingConnection)
            {
                await Task.Delay(timeToWait);
                throw new TimeoutException();
            }

            ConnectionInUse.Reset();

            return new DisposeOnce<SingleAttempt>(() =>
            {
                while (_recentConnections.Count > 10)
                {
                    _recentConnections.TryDequeue(out SubscriptionConnectionBase _);
                }
                _recentConnections.Enqueue(incomingConnection);
                Interlocked.CompareExchange(ref _currentConnection, null, incomingConnection);
                ConnectionInUse.Set();
            });
        }

        public void RegisterRejectedConnection(SubscriptionConnectionBase connection, SubscriptionException exception = null)
        {
            if (exception != null && connection.ConnectionException == null)
                connection.ConnectionException = exception;

            while (_rejectedConnections.Count > 10)
            {
                _rejectedConnections.TryDequeue(out SubscriptionConnectionBase _);
            }
            _rejectedConnections.Enqueue(connection);
        }

        public IEnumerable<SubscriptionConnectionBase> RecentConnections => _recentConnections;
        public IEnumerable<SubscriptionConnectionBase> RecentRejectedConnections => _rejectedConnections;
        public ConcurrentSet<SubscriptionConnectionBase> PendingConnections => _pendingConnections;

        public string SubscriptionName { get; }

        public SubscriptionConnectionBase MostRecentEndedConnection()
        {
            if (_recentConnections.TryPeek(out var recentConnection))
                return recentConnection;
            return null;
        }

        public void EndConnection()
        {
            _currentConnection?.CancellationTokenSource.Cancel();
        }

        public void Dispose()
        {
            try
            {
                EndConnection();
            }
            catch
            {
                // ignored: If we've failed to raise the cancellation token, it means that it's already raised
            }
        }
        
        public void CleanupRecentAndRejectedConnections()
        {
            _recentConnections.Clear();
            _rejectedConnections.Clear();
        }
    }
}
