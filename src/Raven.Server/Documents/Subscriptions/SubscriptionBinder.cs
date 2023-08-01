using System;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Sparrow.Json;

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
        out ISubscriptionConnection connection)
    {
        if (databaseResult.DatabaseStatus == DatabasesLandlord.DatabaseSearchResult.Status.Sharded)
        {
            var orch = new OrchestratedSubscriptionConnection(server, databaseResult.DatabaseContext.SubscriptionsStorage, tcpConnectionOptions, onDispose, buffer);
            connection = orch;
            return new SubscriptionBinder<SubscriptionConnectionsStateOrchestrator, OrchestratedSubscriptionConnection, OrchestratorIncludesCommandImpl>(
                tcpConnectionOptions.DatabaseContext.SubscriptionsStorage,
                new Lazy<SubscriptionConnectionsStateOrchestrator>(orch.GetOrchestratedSubscriptionConnectionState), orch);
        }

        if (tcpConnectionOptions.DocumentDatabase is ShardedDocumentDatabase shardedDocumentDatabase)
        {
            var connectionForShard = new SubscriptionConnectionForShard(server, tcpConnectionOptions, onDispose, buffer,
                shardedDocumentDatabase.ShardedDatabaseName);
            connection = connectionForShard;
            return new SubscriptionBinder<SubscriptionConnectionsState, SubscriptionConnection, DatabaseIncludesCommandImpl>(tcpConnectionOptions.DocumentDatabase.SubscriptionStorage,
                new Lazy<SubscriptionConnectionsState>(connectionForShard.GetSubscriptionConnectionStateForShard), connectionForShard);
        }

        var nonShardedConnection = new SubscriptionConnection(server, tcpConnectionOptions, onDispose, buffer,
            tcpConnectionOptions.DocumentDatabase.Name);
        connection = nonShardedConnection;

        return new SubscriptionBinder<SubscriptionConnectionsState, SubscriptionConnection, DatabaseIncludesCommandImpl>(tcpConnectionOptions.DocumentDatabase.SubscriptionStorage,
            new Lazy<SubscriptionConnectionsState>(nonShardedConnection.GetSubscriptionConnectionState), nonShardedConnection);
    }
}

public sealed class SubscriptionBinder<TState, TConnection, TIncludeCommand> : ISubscriptionBinder
    where TState : AbstractSubscriptionConnectionsState<TConnection, TIncludeCommand>
    where TConnection : SubscriptionConnectionBase<TIncludeCommand>
    where TIncludeCommand : AbstractIncludesCommand
{
    private readonly AbstractSubscriptionStorage<TState> _storage;
    private readonly Lazy<TState> _state;
    private readonly TConnection _connection;

    private TState _subscriptionConnectionsState => _state.Value;

    public SubscriptionBinder(AbstractSubscriptionStorage<TState> storage, Lazy<TState> state, TConnection connection)
    {
        _storage = storage;
        _state = state;
        _connection = connection;
    }

    public async Task Run(TcpConnectionOptions tcpConnectionOptions, IDisposable subscriptionConnectionInProgress)
    {
        using (tcpConnectionOptions)
        using (subscriptionConnectionInProgress)
        using (_connection)
        {
            var stats = _connection.Stats;
            stats.Initialize();
            IDisposable disposeOnDisconnect = default;

            try
            {
                await _connection.ParseSubscriptionOptionsAsync();

                if (_storage.TryEnterSubscriptionsSemaphore() == false)
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
                    await _connection.ProcessSubscriptionAsync<TState, TConnection>(_subscriptionConnectionsState);
                }
                finally
                {
                    _storage.ReleaseSubscriptionsSemaphore();
                }
            }
            catch (Exception e)
            {
                await _subscriptionConnectionsState.HandleConnectionExceptionAsync(_connection, e);
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
        _connection.Stats.CreateActiveConnectionScope();

        // update the state if above data changed
        _subscriptionConnectionsState.Initialize(_connection, afterSubscribe: true);

        await _connection.SendNoopAckAsync();
        await _connection.SendAcceptMessageAsync();

        await _subscriptionConnectionsState.UpdateClientConnectionTime();
    }
}
