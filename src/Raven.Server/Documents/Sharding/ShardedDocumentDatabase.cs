using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding;

public class ShardedDocumentDatabase : DocumentDatabase
{
    public readonly int ShardNumber;

    public readonly string ShardedDatabaseName;

    public string ShardedDatabaseId { get; private set; }

    public ShardedDocumentsStorage ShardedDocumentsStorage;

    public ShardedDatabaseContext DatabaseContext => ServerStore.DatabasesLandlord.GetShardedDatabaseContext(ShardedDatabaseName);

    public ShardedDocumentDatabase(string name, RavenConfiguration configuration, ServerStore serverStore, Action<string> addToInitLog) : 
        base(name, configuration, serverStore, addToInitLog)
    {
        ShardNumber = ShardHelper.GetShardNumber(name);
        ShardedDatabaseName = ShardHelper.ToDatabaseName(name);
    }

    protected override byte[] ReadSecretKey(TransactionOperationContext context) => ServerStore.GetSecretKey(context, ShardedDatabaseName);

    protected override void InitializeSubscriptionStorage()
    {
        SubscriptionStorage.Initialize(ShardedDatabaseName);
    }

    protected override void InitializeCompareExchangeStorage()
    {
        CompareExchangeStorage.Initialize(ShardedDatabaseName);
    }

    protected override DocumentsStorage CreateDocumentsStorage(Action<string> addToInitLog)
    {
        return ShardedDocumentsStorage = new ShardedDocumentsStorage(this, addToInitLog);
    }

    internal override void SetIds(DatabaseTopology topology, string shardedDatabaseId)
    {
        base.SetIds(topology, shardedDatabaseId);
        ShardedDatabaseId = shardedDatabaseId;
    }

    protected override void OnDatabaseRecordChanged(DatabaseRecord record)
    {
        // this called under lock
        base.OnDatabaseRecordChanged(record);

        if (ServerStore.Sharding.ManualMigration)
            return;

        var finishedMigrations = record.Sharding.BucketMigrations.Where(m => m.Value.Status == MigrationStatus.OwnershipTransferred);
        foreach (var finishedMigration in finishedMigrations)
        {
            var migration = finishedMigration.Value;
            if (migration.SourceShard == ShardNumber && migration.ConfirmedSourceCleanup.Contains(ServerStore.NodeTag) == false)
            {
                // cleanup values
                _ = DeleteBucket(migration.Bucket, migration.MigrationIndex, migration.LastSourceChangeVector);
            }
        }
    }

    public ShardingConfiguration ReadShardingState()
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var raw = ServerStore.Cluster.ReadRawDatabaseRecord(context, ShardedDatabaseName);
            return raw.Sharding.Value;
        }
    }

    protected override ClusterTransactionBatchCollector CollectCommandsBatch(TransactionOperationContext context, int take)
    {
        var batchCollector = new ShardedClusterTransactionBatchCollector(this, take);
        var readCommands = ClusterTransactionCommand.ReadCommandsBatch(context, ShardedDatabaseName, fromCount: _nextClusterCommand, take: take);
                
        foreach (var command in readCommands)
        {
            batchCollector.MaxIndex = command.Index; 
            batchCollector.MaxCommandCount = command.PreviousCount + command.Commands.Length;
            if (command.ShardNumber == ShardNumber)
                batchCollector.Add(command);
        }

        return batchCollector;
    }

    protected class ShardedClusterTransactionBatchCollector : ClusterTransactionBatchCollector
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

    public async Task DeleteBucket(int bucket, long migrationIndex, string uptoChangeVector)
    {
        var cmd = new DeleteBucketCommand(bucket, uptoChangeVector);
        while (cmd.HasMore)
        {
            await TxMerger.Enqueue(cmd);
        }

        await ServerStore.Sharding.SourceMigrationCleanup(ShardedDatabaseName, bucket, migrationIndex,
            $"{bucket}@{migrationIndex}-Cleaned-{ServerStore.NodeTag}");
    }

    public static ShardedDocumentDatabase CastToShardedDocumentDatabase(DocumentDatabase database) => database as ShardedDocumentDatabase ?? throw new ArgumentException($"Database {database.Name} must be sharded!");

    private class DeleteBucketCommand : TransactionOperationsMerger.MergedTransactionCommand
    {
        private readonly int _bucket;
        private readonly string _uptoChangeVector;
        public bool HasMore = true;

        public DeleteBucketCommand(int bucket, string uptoChangeVector)
        {
            _bucket = bucket;
            _uptoChangeVector = uptoChangeVector;
        }
        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Critical, "We need to create here proper tombstones so backup can pick it up RavenDB-19197");
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Delete revision/attachments/ etc.. RavenDB-19197");

            var database = context.DocumentDatabase as ShardedDocumentDatabase;
            var result = database.ShardedDocumentsStorage.DeleteBucket(context, _bucket, context.GetChangeVector(_uptoChangeVector));
            HasMore = result > 0;
            return result;
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
        {
            throw new NotImplementedException();
        }
    }
}
