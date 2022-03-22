using System;
using Raven.Client.ServerWide;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents;

public class ShardedDocumentDatabase : DocumentDatabase
{
    public int ShardNumber;

    public string ShardedDatabaseName;

    public string ShardedDatabaseId;

    public ShardedDocumentDatabase(string name, RavenConfiguration configuration, ServerStore serverStore, Action<string> addToInitLog) : 
        base(name, configuration, serverStore, addToInitLog)
    {
        ShardNumber = ShardHelper.TryGetShardIndex(name);
        ShardedDatabaseName = ShardHelper.ToDatabaseName(name);
    }

    protected override byte[] ReadSecretKey(TransactionOperationContext context) => ServerStore.GetSecretKey(context, ShardedDatabaseName);

    protected override void InitializeSubscriptionStorage()
    {
        SubscriptionStorage.Initialize(ShardedDatabaseName);
    }

    protected override void SetIds(DatabaseTopology topology, string shardedDatabaseId)
    {
        base.SetIds(topology, shardedDatabaseId);
        ShardedDatabaseId = shardedDatabaseId;
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
}
