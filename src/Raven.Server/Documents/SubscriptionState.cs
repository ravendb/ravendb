using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Extensions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Utils.Metrics;
using Sparrow;

namespace Raven.Server.Documents
{
    public class SubscriptionState:IDisposable
    {
        private readonly AsyncManualResetEvent _connectionInUse = new AsyncManualResetEvent();

        public SubscriptionState(SubscriptionConnection currentConnection)
        {
            _currentConnection = currentConnection;
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
            int timeToWait)
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

                            _currentConnection.ConnectionException = new SubscriptionClosedException("Closed by Takeover");
                            _currentConnection?.CancellationTokenSource.Cancel();
                        
                            throw new TimeoutException();
                        case SubscriptionOpeningStrategy.ForceAndKeep:
                            _currentConnection.ConnectionException = new SubscriptionClosedException("Closed by ForceAndKeep");
                            _currentConnection?.CancellationTokenSource.Cancel();
                        
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

            _connectionInUse.Reset();
            _currentConnection = incomingConnection;
            return new DisposableAction(() => {
                while (_recentConnections.Count > 10)
                {
                    SubscriptionConnection options;
                    _recentConnections.TryDequeue(out options);
                }
                _recentConnections.Enqueue(incomingConnection);
                _connectionInUse.SetByAsyncCompletion();
                _currentConnection = null;
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