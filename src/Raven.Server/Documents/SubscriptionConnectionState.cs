using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Extensions;
using Raven.Client.Extensions;
using Raven.Server.Utils.Metrics;
using Sparrow;
using Sparrow.Collections;

namespace Raven.Server.Documents
{

    
    public class SubscriptionConnectionState
    {
        private readonly AsyncManualResetEvent _connectionInUse = new AsyncManualResetEvent();
        
        public SubscriptionConnectionState(SubscriptionConnectionOptions currentConnection)
        {
            _currentConnection = currentConnection;
            _connectionInUse.Set();
        }

        private SubscriptionConnectionOptions _currentConnection;

        private readonly ConcurrentSet<string> _forciblyClosedConnections = new ConcurrentSet<string>();

        public SubscriptionConnectionOptions Connection => _currentConnection;


        // we should have two locks: one lock for a connection and one lock for operations
        // remember to catch ArgumentOutOfRangeException for timeout problems
        public async Task<IDisposable> RegisterSubscriptionConnection(
            SubscriptionConnectionOptions incomingConnection,
            int timeToWait)
        {
            if (_forciblyClosedConnections.Contains(incomingConnection.ConnectionId))
                throw new SubscriptionClosedException(
                    $"Subscription with ID {incomingConnection.SubscriptionId} and connection id {incomingConnection.ConnectionId} was forcibly closed before and therefore cannot be reopened");
            if (await _connectionInUse.WaitAsync(timeToWait) == false)
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
                            throw new SubscriptionInUseException(
                                $"Subscription {incomingConnection.SubscriptionId} is occupied by a ForceAndKeep connection, connectionId cannot be opened");
                        _currentConnection?.CancellationTokenSource.Cancel();
                        throw new TimeoutException();
                    case SubscriptionOpeningStrategy.ForceAndKeep:
                        _currentConnection?.CancellationTokenSource.Cancel();
                        throw new TimeoutException();
                    default:
                        throw new InvalidOperationException("Unknown subscription open strategy: " +
                                                            incomingConnection.Strategy);
                }
            }

            await WaitOnCriticalSection();
            _connectionInUse.Reset();
            _currentConnection = incomingConnection;
            return new DisposableAction(() =>
            {
                _connectionInUse.SetByAsyncCompletion();
            });
        }

        public void EndConnection()
        {
            _currentConnection?.CancellationTokenSource.Cancel();
        }
    }
}