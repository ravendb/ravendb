using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Sharding;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.Subscriptions.SubscriptionProcessor;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Threading;
using Sparrow.Utils;
using static Raven.Server.Documents.Subscriptions.SubscriptionProcessor.AbstractSubscriptionProcessorBase;

namespace Raven.Server.Documents.TcpHandlers;

public class SubscriptionConnectionForShard : SubscriptionConnection
{
    public readonly string ShardName;
    private readonly ShardedDocumentDatabase _shardedDatabase;
    private readonly HashSet<string> _dbIdsToRemove;
    private SubscriptionConnectionsStateForShard _state;
    private ShardedDocumentsDatabaseSubscriptionProcessor _processor;

    public SubscriptionConnectionForShard(ServerStore serverStore, TcpConnectionOptions tcpConnection, IDisposable tcpConnectionDisposable, JsonOperationContext.MemoryBuffer bufferToCopy, string database) :
        base(serverStore, tcpConnection, tcpConnectionDisposable, bufferToCopy, database)
    {
        _shardedDatabase = (ShardedDocumentDatabase)tcpConnection.DocumentDatabase;
        ShardName = tcpConnection.DocumentDatabase.Name;
        _dbIdsToRemove = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { _shardedDatabase.ShardedDatabaseId };
    }

    protected override StatusMessageDetails GetDefault()
    {
        return new StatusMessageDetails
        {
            DatabaseName = $"for shard '{ShardName}'",
            ClientType = "'sharded worker'",
            SubscriptionType = "sharded subscription"
        };
    }

    protected override DynamicJsonValue AcceptMessage()
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19085 need to ensure the sharded workers has the same sub definition. by sending my raft index?");
        return base.AcceptMessage();
    }

    protected override RawDatabaseRecord GetRecord(ClusterOperationContext context) => ServerStore.Cluster.ReadRawDatabaseRecord(context, ShardName);


    public override AbstractSubscriptionProcessor<DatabaseIncludesCommandImpl> CreateProcessor(SubscriptionConnectionBase<DatabaseIncludesCommandImpl> connection)
    {
        if (connection is SubscriptionConnectionForShard shardConnection)
        {
            var database = (ShardedDocumentDatabase)connection.TcpConnection.DocumentDatabase;
            var server = database.ServerStore;

            if (connection.Subscription.Revisions)
            {
                return new ShardedRevisionsDatabaseSubscriptionProcessor(server, database, shardConnection);
            }

            _processor = new ShardedDocumentsDatabaseSubscriptionProcessor(server, database, shardConnection);
            return _processor;
        }

        throw new InvalidOperationException($"Expected to create a processor for '{nameof(SubscriptionConnectionForShard)}', but got: '{connection.GetType().Name}'.");
    }

    protected override async Task<BatchStatus> TryRecordBatchAndUpdateStatusAsync(IChangeVectorOperationContext context, SubscriptionBatchResult result)
    {
        if (result.Status == BatchStatus.ActiveMigration)
        {
            if (result.CurrentBatch.Count == 0)
            {
                // we didn't pull anything and there is migration, we don't update the cv and will wait for migration to complete
                return BatchStatus.ActiveMigration;
            }

            // we already pulled some docs, we will send the docs in CurrentBatch and then will wait for migration to complete
        }

        var vector = context.GetChangeVector(result.LastChangeVectorSentInThisBatch);
        vector.TryRemoveIds(_dbIdsToRemove, context, out vector);
        var cvBeforeRecordBatch = LastSentChangeVectorInThisConnection;
        result.LastChangeVectorSentInThisBatch = vector.Order;
        try
        {
            await base.TryRecordBatchAndUpdateStatusAsync(context, result);
        }
        catch (DocumentUnderActiveMigrationException e)
        {
            // one of the batch docs is under active migration
            // we canceled this batch and wait for migration to complete 
            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Got '{nameof(DocumentUnderActiveMigrationException)}' on shard '{ShardName}' will roll back the change vector from '{LastSentChangeVectorInThisConnection}' to '{cvBeforeRecordBatch}', and wait for migration to complete.", e);
            }
            LastSentChangeVectorInThisConnection = cvBeforeRecordBatch;
            return BatchStatus.ActiveMigration;
        }

        if (_processor.Skipped != null)
        {
            for (int i = result.CurrentBatch.Count - 1; i >= 0; i--)
            {
                var item = result.CurrentBatch[i];
                if (_processor.Skipped.Contains(item.Document.Id))
                {
                    result.CurrentBatch.RemoveAt(i);
                    item.Document.Dispose();
                }
            }

            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info,
                $"Skipped '{_processor.Skipped.Count}' docs after apply '{nameof(RecordBatchSubscriptionDocumentsCommand)}' command."));

            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Skipped '{_processor.Skipped.Count}' docs after apply '{nameof(RecordBatchSubscriptionDocumentsCommand)}' command. Skipped docs:{Environment.NewLine}{string.Join(",", _processor.Skipped)}");
            }
        }

        if (result.CurrentBatch.Count == 0 && result.Status == BatchStatus.ActiveMigration)
        {
            // all current batch items were skipped, relevant for shard after resharding
            return BatchStatus.ActiveMigration;
        }

        if (result.CurrentBatch.Count == 0)
        {
            // empty batch
            return BatchStatus.EmptyBatch;
        }

        return BatchStatus.DocumentsSent;
    }

    protected override bool FoundAboutMoreDocs()
    {
        if (base.FoundAboutMoreDocs())
            return true;

        if (_state.HasDocumentFromResend())
            return true;

        return false;
    }

    public SubscriptionConnectionsState GetSubscriptionConnectionStateForShard()
    {
        var subscriptions = TcpConnection.DocumentDatabase.SubscriptionStorage.Subscriptions;
        var state = subscriptions.GetOrAdd(SubscriptionId, subId => new SubscriptionConnectionsStateForShard(DatabaseName, subId, TcpConnection.DocumentDatabase.SubscriptionStorage));
        State = state;
        _state = (SubscriptionConnectionsStateForShard)state;
        return state;
    }

    protected override void FillIncludedDocuments(DatabaseIncludesCommandImpl includeDocumentsCommand, List<Document> includes)
    {
        includeDocumentsCommand.IncludeDocumentsCommand.Fill(includes, includeMissingAsNull: true);
    }

    internal override async Task HandleBatchStatus<TState, TConnection>(TState state, BatchStatus status, Stopwatch sendingCurrentBatchStopwatch, DisposeOnce<SingleAttempt> markInUse, SubscriptionBatchStatsScope batchScope)
    {
        if (status == BatchStatus.ActiveMigration)
        {
            await LogBatchStatusAndUpdateStatsAsync(sendingCurrentBatchStopwatch, $"Subscription '{Options.SubscriptionName}' is '{nameof(BatchStatus.ActiveMigration)}'");

            if (await WaitForDocsMigrationAsync(state, _lastReplyFromClientTask))
            {
                // we waited for Migration and will try to pull the docs again
                return;
            }

            await CancelSubscriptionAndThrowAsync();
        }

        await base.HandleBatchStatus<TState, TConnection>(state, status, sendingCurrentBatchStopwatch, markInUse, batchScope);
    }

    private async Task<bool> WaitForDocsMigrationAsync(AbstractSubscriptionConnectionsState state, Task pendingReply)
    {
        AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Start waiting for documents migration"));
        var migrationWaitTask = TimeoutManager.WaitFor(TimeSpan.FromSeconds(5));
        do
        {
            CancellationTokenSource.Token.ThrowIfCancellationRequested();

            var resultingTask = await Task
                .WhenAny(migrationWaitTask, pendingReply, TimeoutManager.WaitFor(ISubscriptionConnection.HeartbeatTimeout)).ConfigureAwait(false);

            if (CancellationTokenSource.IsCancellationRequested)
                return false;
            if (resultingTask == pendingReply)
                return false;
            if (migrationWaitTask == resultingTask)
                return true;

            await SendHeartBeatAsync("Waiting for documents migration");
            await SendNoopAckAsync();
        } while (CancellationTokenSource.IsCancellationRequested == false);

        return false;
    }
}
