using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Subscriptions;

public interface ISubscriptionBinder
{ 
    Task Run(TcpConnectionOptions tcpConnectionOptions, IDisposable subscriptionConnectionInProgress);
}

public static class SubscriptionBinder
{
    public static ISubscriptionBinder CreateSubscriptionBinder(
        ServerStore server,
        DatabasesLandlord.DatabaseSearchResult databaseResult,
        TcpConnectionOptions tcpConnectionOptions,
        JsonOperationContext.MemoryBuffer buffer,
        IDisposable onDispose,
        out SubscriptionConnectionBase connection)
    {
        if (databaseResult.DatabaseStatus == DatabasesLandlord.DatabaseSearchResult.Status.Sharded)
        {
            var orch = new OrchestratedSubscriptionConnection(server, tcpConnectionOptions, onDispose, buffer);
            connection = orch;
            return new SubscriptionBinder<SubscriptionConnectionsStateOrchestrator, OrchestratedSubscriptionConnection>(
                tcpConnectionOptions.DatabaseContext.Subscriptions,
                new Lazy<SubscriptionConnectionsStateOrchestrator>(orch.GetOrchestratedSubscriptionConnectionState), orch);
        }

        if (tcpConnectionOptions.DocumentDatabase is ShardedDocumentDatabase shardedDocumentDatabase)
        {
            var connectionForShard = new SubscriptionConnectionForShard(server, tcpConnectionOptions, onDispose, buffer,
                shardedDocumentDatabase.ShardedDatabaseName);
            connection = connectionForShard;
            return new SubscriptionBinder<SubscriptionConnectionsState, SubscriptionConnection>(tcpConnectionOptions.DocumentDatabase.SubscriptionStorage,
                new Lazy<SubscriptionConnectionsState>(connectionForShard.GetSubscriptionConnectionStateForShard), connectionForShard);
        }

        var connection2 = new SubscriptionConnection(server, tcpConnectionOptions, onDispose, buffer,
            tcpConnectionOptions.DocumentDatabase.Name);
        connection = connection2;

        return new SubscriptionBinder<SubscriptionConnectionsState, SubscriptionConnection>(tcpConnectionOptions.DocumentDatabase.SubscriptionStorage,
            new Lazy<SubscriptionConnectionsState>(connection2.GetSubscriptionConnectionState), connection2);
    }
}

public class SubscriptionBinder<TState, TConnection> : ISubscriptionBinder
    where TState : SubscriptionConnectionsStateBase<TConnection>
    where TConnection : SubscriptionConnectionBase
{
    private readonly ISubscriptionSemaphore _semaphore;
    private readonly Lazy<TState> _state;
    private readonly TConnection _connection;

    private TState _subscriptionConnectionsState => _state.Value;

    public SubscriptionBinder(ISubscriptionSemaphore semaphore, Lazy<TState> state, TConnection connection)
    {
        _semaphore = semaphore;
        _state = state;
        _connection = connection;
    }

    public async Task Run(TcpConnectionOptions tcpConnectionOptions, IDisposable subscriptionConnectionInProgress)
    {
        using (tcpConnectionOptions)
        using (subscriptionConnectionInProgress)
        using (_connection)
        {
            var stats = _connection.StatsCollector;
            stats.Initialize();
            IDisposable disposeOnDisconnect = default;

            try
            {
                await _connection.ParseSubscriptionOptionsAsync();

                if (_semaphore.TryEnterSubscriptionsSemaphore() == false)
                {
                    throw new SubscriptionClosedException(
                        $"Cannot open new subscription connection, max amount of concurrent connections reached ({_connection.TcpConnection.DocumentDatabase.Configuration.Subscriptions.MaxNumberOfConcurrentConnections}), you can modify the value at 'Subscriptions.MaxNumberOfConcurrentConnections'");
                }

                try
                {
                    long registerConnectionDurationInTicks;
                    using (stats.PendingConnectionScope)
                    {
                        await _connection.InitAsync();
                        _subscriptionConnectionsState.Initialize(_connection);
                        (disposeOnDisconnect, registerConnectionDurationInTicks) = await _subscriptionConnectionsState.SubscribeAsync(_connection);
                    }

                    await NotifyClientAboutSuccessAsync(registerConnectionDurationInTicks);
                    await _connection.ProcessSubscriptionAsync();
                }
                finally
                {
                    _semaphore.ReleaseSubscriptionsSemaphore();
                }
            }
            catch (SubscriptionChangeVectorUpdateConcurrencyException e)
            {
                _subscriptionConnectionsState.DropSubscription(e);
                await _connection.ReportExceptionAsync(SubscriptionError.Error, e);
            }
            catch (SubscriptionInUseException e)
            {
                await _connection.ReportExceptionAsync(SubscriptionError.ConnectionRejected, e);
            }
            catch (Exception e)
            {
                await _connection.ReportExceptionAsync(SubscriptionError.Error, e);
            }
            finally
            {
                _connection.FinishProcessing();
                disposeOnDisconnect?.Dispose();
            }
        }
    }

    private async Task NotifyClientAboutSuccessAsync(long registerConnectionDurationInTicks)
    {
        _connection.TcpConnection.DocumentDatabase?.ForTestingPurposes?.Subscription_ActionToCallAfterRegisterSubscriptionConnection?.Invoke(registerConnectionDurationInTicks);

        // refresh subscription data (change vector may have been updated, because in the meanwhile, another subscription could have just completed a batch)
        await _connection.RefreshAsync(registerConnectionDurationInTicks);
        _connection.StatsCollector.CreateActiveConnectionScope();

        // update the state if above data changed
        _subscriptionConnectionsState.Initialize(_connection, afterSubscribe: true);

        await _connection.SendNoopAckAsync();
        await _connection.WriteJsonAsync(new DynamicJsonValue
        {
            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
        });

        await _subscriptionConnectionsState.UpdateClientConnectionTime();
    }
}
