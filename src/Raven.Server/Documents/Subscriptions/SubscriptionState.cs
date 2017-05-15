using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Exceptions.Subscriptions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.Documents.TcpHandlers;
using Sparrow;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionState:IDisposable
    {
        private readonly SubscriptionStorage _storage;
        private readonly AsyncManualResetEvent _connectionInUse = new AsyncManualResetEvent();

        public SubscriptionState(SubscriptionStorage storage)
        {
            _storage = storage;
            _connectionInUse.Set();
        }

        private SubscriptionConnection _currentConnection;
      
        
        private readonly ConcurrentQueue<SubscriptionConnection> _recentConnections = new ConcurrentQueue<SubscriptionConnection>();
        private readonly ConcurrentQueue<SubscriptionConnection> _rejectedConnections = new ConcurrentQueue<SubscriptionConnection>();


        public SubscriptionConnection Connection => _currentConnection;
        


        // we should have two locks: one lock for a connection and one lock for operations
        // remember to catch ArgumentOutOfRangeException for timeout problems
        public async Task<IDisposable> RegisterSubscriptionConnection(
            SubscriptionConnection incomingConnection,
            uint timeToWait)
        {
            try
            {
                if (await _connectionInUse.WaitAsync(TimeSpan.FromMilliseconds(timeToWait)) == false)
                {
                    switch (incomingConnection.Strategy)
                    {
                        // we try to connect, if the resource is occupied, we will throw an exception
                        // this piece of code could have been upper, but we choose to have it here, for better readability
                        case SubscriptionOpeningStrategy.WaitForFree:
                            throw new TimeoutException();

                        case SubscriptionOpeningStrategy.OpenIfFree:
                            throw new SubscriptionInUseException(
                                $"Subscription {incomingConnection.SubscriptionId} is occupied, connection cannot be opened");

                        case SubscriptionOpeningStrategy.TakeOver:
                            if (_currentConnection?.Strategy == SubscriptionOpeningStrategy.ForceAndKeep)
                                throw  new SubscriptionInUseException(
                                    $"Subscription {incomingConnection.SubscriptionId} is occupied by a ForceAndKeep connection, connectionId cannot be opened");

                            if (_currentConnection != null)
                                _storage.DropSubscriptionConnection(_currentConnection.SubscriptionId,
                                    "Closed by TakeOver");

                            throw new TimeoutException();

                        case SubscriptionOpeningStrategy.ForceAndKeep:

                            if (_currentConnection != null)
                                _storage.DropSubscriptionConnection(_currentConnection.SubscriptionId,
                                    "Closed by ForceAndKeep");

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
                throw new TimeoutException();

            _connectionInUse.Reset();

            return new DisposableAction(() => {
                while (_recentConnections.Count > 10)
                {
                    SubscriptionConnection options;
                    _recentConnections.TryDequeue(out options);
                }
                _recentConnections.Enqueue(incomingConnection);
                _connectionInUse.SetByAsyncCompletion();
                Interlocked.CompareExchange(ref _currentConnection, null, incomingConnection);
            });
        }

        public void RegisterRejectedConnection(SubscriptionConnection connection, SubscriptionException exception = null)
        {
            if (exception!= null && connection.ConnectionException == null)
                connection.ConnectionException = exception;

            while (_rejectedConnections.Count > 10)
            {
                SubscriptionConnection options;
                _rejectedConnections.TryDequeue(out options);
            }
            _rejectedConnections.Enqueue(connection);
        }

        public SubscriptionConnection[] RecentConnections => _recentConnections.ToArray();
        public SubscriptionConnection[] RecentRejectedConnections => _rejectedConnections.ToArray();

        public void EndConnection()
        {
            _currentConnection?.CancellationTokenSource.Cancel();
        }

        public void Dispose()
        {
            EndConnection();
        }
    }
}