using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Config;
using Raven.Server.Documents.Commands;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Sharding;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Sharding.Background;
using Raven.Server.Documents.Sharding.Smuggler;
using Raven.Server.Documents.Subscriptions.Sharding;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding;

public sealed class ShardedDocumentDatabase : DocumentDatabase
{
    private readonly Logger _logger;

    public readonly int ShardNumber;
    
    public readonly string ShardedDatabaseName;

    public string ShardedDatabaseId { get; private set; }

    public ShardedDocumentsStorage ShardedDocumentsStorage;

    public ShardedDocumentsMigrator DocumentsMigrator { get; private set; }

    public ShardedDocumentDatabase(string name, RavenConfiguration configuration, ServerStore serverStore, Action<string> addToInitLog)
        : base(name, configuration, serverStore, addToInitLog)
    {
        ShardNumber = ShardHelper.GetShardNumberFromDatabaseName(name);
        ShardedDatabaseName = ShardHelper.ToDatabaseName(name);
        Smuggler = new ShardedDatabaseSmugglerFactory(this);

        _logger = LoggingSource.Instance.GetLogger<ShardedDocumentDatabase>(Name);
    }

    protected override byte[] ReadSecretKey(TransactionOperationContext context) => ServerStore.GetSecretKey(context, ShardedDatabaseName);

    protected override void InitializeCompareExchangeStorage()
    {
        CompareExchangeStorage.Initialize(ShardedDatabaseName);
    }

    protected override void InitializeAndStartDocumentsMigration()
    {
        DocumentsMigrator = new ShardedDocumentsMigrator(this);
        _ = DocumentsMigrator.ExecuteMoveDocumentsAsync();
    }

    protected override DocumentsStorage CreateDocumentsStorage(Action<string> addToInitLog)
    {
        return ShardedDocumentsStorage = new ShardedDocumentsStorage(this, addToInitLog);
    }

    protected override IndexStore CreateIndexStore(ServerStore serverStore)
    {
        return new ShardedIndexStore(this, serverStore);
    }

    protected override ReplicationLoader CreateReplicationLoader()
    {
        return new ShardReplicationLoader(this, ServerStore);
    }

    protected override ShardSubscriptionStorage CreateSubscriptionStorage(ServerStore serverStore)
    {
        return new ShardSubscriptionStorage(this, serverStore, ShardHelper.ToDatabaseName(Name));
    }

    internal override void SetIds(DatabaseTopology topology, string shardedDatabaseId)
    {
        base.SetIds(topology, shardedDatabaseId);
        ShardedDatabaseId = shardedDatabaseId;
    }

    public ShardingConfiguration ShardingConfiguration;

    private ConcurrentDictionary<long, Task> _confirmations = new ConcurrentDictionary<long, Task>();

    protected override void OnDatabaseRecordChanged(DatabaseRecord record)
    {
        // this called under lock
        base.OnDatabaseRecordChanged(record);

        ShardingConfiguration = record.Sharding;

        if (ServerStore.Sharding.ManualMigration)
            return;

        HandleReshardingChanges();
    }

    public void HandleReshardingChanges()
    {
        foreach (var migration in ShardingConfiguration.BucketMigrations)
        {
            var process = migration.Value;

            Task t = null;
            var index = 0L;
            if (ShardNumber == process.DestinationShard && process.Status == MigrationStatus.Moved)
            {
                if (process.ConfirmedDestinations.Contains(ServerStore.NodeTag) == false)
                {
                    using (DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var current = ShardedDocumentsStorage.GetMergedChangeVectorInBucket(context, process.Bucket);
                        var status = ChangeVector.GetConflictStatusForDocument(context.GetChangeVector(process.LastSourceChangeVector), current);
                        if (status == ConflictStatus.AlreadyMerged)
                        {
                            index = process.MigrationIndex;

                            if (_confirmations.TryGetValue(process.MigrationIndex, out t))
                            {
                                if (t.IsCompleted == false)
                                    continue;
                            }

                            t = ServerStore.Sharding.DestinationMigrationConfirm(ShardedDatabaseName, process.Bucket, process.MigrationIndex);
                        }
                    }
                }
            }

            if (process.SourceShard == ShardNumber && process.Status == MigrationStatus.OwnershipTransferred)
            {
                index = (long)Hashing.XXHash64.CalculateRaw(process.LastSourceChangeVector ?? $"No docs for {process.MigrationIndex}");

                if (_confirmations.TryGetValue(index, out t))
                {
                    if (t.IsCompleted == false)
                        continue;
                }

                Debug.Assert(process.ConfirmationIndex.HasValue, $"invalid ShardBucketMigration for bucket '{process.Bucket}', " +
                                                                 "got Status = OwnershipTransferred but no ConfirmationIndex");

                // cleanup values
                t = DeleteBucketAsync(process.Bucket, process.MigrationIndex, process.ConfirmationIndex.Value, process.LastSourceChangeVector);

                t.ContinueWith(__ => DocumentsMigrator.ExecuteMoveDocumentsAsync(), TaskContinuationOptions.NotOnFaulted);
            }

            if (t != null)
            {
                _confirmations[index] = t;
                t.ContinueWith(__ => _confirmations.TryRemove(index, out _), TaskContinuationOptions.NotOnFaulted);
            }
        }
    }

    protected override ClusterTransactionBatchCollector CollectCommandsBatch(ClusterOperationContext context, long lastCompletedClusterTransactionIndex, int take)
    {
        var batchCollector = new ShardedClusterTransactionBatchCollector(this, take);
        var readCommands = ClusterTransactionCommand.ReadCommandsBatch(context, ShardedDatabaseName, fromCount: _nextClusterCommand, lastCompletedClusterTransactionIndex, take);

        foreach (var command in readCommands)
        {
            batchCollector.MaxIndex = command.Index;
            batchCollector.MaxCommandCount = command.PreviousCount + command.Commands.Count;
            if (command.ShardNumber == ShardNumber)
                batchCollector.Add(command);
        }

        return batchCollector;
    }

    private sealed class ShardedClusterTransactionBatchCollector : ClusterTransactionBatchCollector
    {
        private readonly ShardedDocumentDatabase _database;

        public long MaxIndex = -1;
        public long MaxCommandCount = -1;

        public ShardedClusterTransactionBatchCollector(ShardedDocumentDatabase database, int maxSize) : base(maxSize)
        {
            _database = database;
        }

        public override void Dispose()
        {
            base.Dispose();
            if (Count == 0 || AllCommandsBeenProcessed)
            {
                if (MaxIndex >= 0)
                {
                    _database.RachisLogIndexNotifications.NotifyListenersAbout(MaxIndex, null);
                    _database._nextClusterCommand = MaxCommandCount;
                }
            }
        }
    }

    public async Task DeleteBucketAsync(int bucket, long migrationIndex, long confirmationIndex, string uptoChangeVector)
    {
        if (string.IsNullOrEmpty(uptoChangeVector))
        {
            await ServerStore.Sharding.SourceMigrationCleanup(ShardedDatabaseName, bucket, migrationIndex);
            return;
        }

        // before starting cleanup, wait for DestinationMigrationConfirm command
        // to be applied in all orchestrator nodes
        await WaitForOrchestratorConfirmationAsync(confirmationIndex);

        var delay = TimeSpan.FromSeconds(1);
        while (true)
        {
            var cmd = new DeleteBucketCommand(this, bucket, uptoChangeVector);
            try
            {
                await TxMerger.Enqueue(cmd);
            }
            catch (Exception exception)
            {
                // if an error occurs during the execution of DeleteBucketCommand,
                // we need to handle it gracefully to avoid rapid retry attempts

                RaiseNotificationOnDeleteBucketFailure(bucket, exception);

                if (delay >= TimeSpan.FromMinutes(5))
                {
                    // if the delay exceeds the maximum timeout, throw an exception and break the loop
                    await Task.FromException(exception);
                    return;
                }

                await Task.Delay(delay, cancellationToken: DatabaseShutdown);
                delay = delay.Multiply(2);
                continue;
            }

            switch (cmd.Result)
            {
                // no documents in the bucket / everything was deleted
                case DeleteBucketCommand.DeleteBucketResult.Empty:
                    await ServerStore.Sharding.SourceMigrationCleanup(ShardedDatabaseName, bucket, migrationIndex);
                    DismissNotificationOnDeleteBucketSuccessIfNeeded();
                    return;
                // some documents skipped and left in the bucket
                case DeleteBucketCommand.DeleteBucketResult.Skipped:
                    return;
                // we have more docs, batch limit reached.
                case DeleteBucketCommand.DeleteBucketResult.FullBatch:
                case DeleteBucketCommand.DeleteBucketResult.ReachedTransactionLimit:
                    delay = TimeSpan.FromSeconds(1);
                    continue;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }

    private string ReshardingFailureNotificationKey => $"{Name}/ReshardingFailure";

    private void RaiseNotificationOnDeleteBucketFailure(int bucket, Exception exception)
    {
        var msg = $"An error occurred while attempting to clean up bucket '{bucket}' from source shard '{ShardNumber}' [{Name}].";

        if (_logger.IsInfoEnabled)
            _logger.Info(msg, exception);

        ServerStore.NotificationCenter.Add(AlertRaised.Create(
            ShardedDatabaseName,
            "Resharding Delay Due to an Error",
            msg,
            AlertType.ClusterTransactionFailure,
            NotificationSeverity.Error,
            details: new ExceptionDetails(exception),
            key: ReshardingFailureNotificationKey));
    }

    private void DismissNotificationOnDeleteBucketSuccessIfNeeded()
    {
        var id = AlertRaised.GetKey(AlertType.ClusterTransactionFailure, ReshardingFailureNotificationKey);
        ServerStore.NotificationCenter.Dismiss(id, sendNotificationEvenIfDoesntExist: false);
    } 

    private async Task WaitForOrchestratorConfirmationAsync(long confirmationIndex)
    {
        var cmd = new WaitForIndexNotificationCommand(new List<long> { confirmationIndex });
        var tasks = new List<Task>(ShardingConfiguration.Orchestrator.Topology.Members.Count);
        var clusterTopology = ServerStore.GetClusterTopology();

        foreach (var nodeTag in ShardingConfiguration.Orchestrator.Topology.Members)
        {
            var chosenNode = new ServerNode
            {
                ClusterTag = nodeTag,
                Database = ShardedDatabaseName,
                ServerRole = ServerNode.Role.Member,
                Url = clusterTopology.GetUrlFromTag(nodeTag)
            };

            var releaseCtx = ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context);
            var t = ServerStore.ClusterRequestExecutor.ExecuteAsync(chosenNode, nodeIndex: null, context, cmd, token: DatabaseShutdown)
                .ContinueWith(_ => releaseCtx.Dispose());
            tasks.Add(t);
        }

        try
        {
            await Task.WhenAll(tasks);
        }
        catch (Exception e)
        {
            throw new InvalidOperationException($"failed to wait for migration confirmation index '{confirmationIndex}' on Orchestrator nodes: {ShardingConfiguration.Orchestrator.Topology}. Error : {e}");
        }
    }

    public static ShardedDocumentDatabase CastToShardedDocumentDatabase(DocumentDatabase database) => database as ShardedDocumentDatabase ?? throw new ArgumentException($"Database {database.Name} must be sharded!");

    public sealed class DeleteBucketCommand : DocumentMergedTransactionCommand
    {
        private readonly ShardedDocumentDatabase _database;
        private readonly int _bucket;
        private readonly string _uptoChangeVector;
        public DeleteBucketResult Result;

        public DeleteBucketCommand(ShardedDocumentDatabase database, int bucket, string uptoChangeVector)
        {
            _database = database;
            _bucket = bucket;
            _uptoChangeVector = uptoChangeVector;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Critical, "We need to create here proper tombstones so backup can pick it up RavenDB-19197");
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Delete revision/attachments/ etc.. RavenDB-19197");

            Result = _database.ShardedDocumentsStorage.DeleteBucket(context, _bucket, context.GetChangeVector(_uptoChangeVector));
            return 1;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
        {
            throw new NotImplementedException();
        }

        public enum DeleteBucketResult
        {
            Empty,
            Skipped,
            FullBatch,
            ReachedTransactionLimit
        }
    }
}
