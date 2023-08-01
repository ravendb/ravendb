using System;
using System.Threading;
using JetBrains.Annotations;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Actions;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Smuggler;

public sealed class ShardedDatabaseDestination : DatabaseDestination
{
    public ShardedDatabaseDestination(ShardedDocumentDatabase database, CancellationToken token = default) 
        : base(database, token)
    {
    }

    protected override ICompareExchangeActions CreateCompareExchangeActions(string databaseName, JsonOperationContext context, BackupKind? backupKind)
    {
        return new ShardedDatabaseCompareExchangeActions(databaseName, _database, context, backupKind, _token);
    }

    private sealed class ShardedDatabaseCompareExchangeActions : DatabaseCompareExchangeActions
    {
        public ShardedDatabaseCompareExchangeActions([NotNull] string databaseName, [NotNull] DocumentDatabase database, JsonOperationContext context, BackupKind? backupKind, CancellationToken token) 
            : base(databaseName, database, context, backupKind, token)
        {
        }

        protected override ClusterTransactionCommand CreateClusterTransactionCommand(string databaseName, char identityPartsSeparator, ArraySegment<ClusterTransactionCommand.ClusterTransactionDataCommand> parsedCommands,
            ClusterTransactionCommand.ClusterTransactionOptions options, string raftRequestId)
        {
            return new ClusterTransactionCommand(databaseName, identityPartsSeparator, parsedCommands, options, raftRequestId);
        }
    }
}
