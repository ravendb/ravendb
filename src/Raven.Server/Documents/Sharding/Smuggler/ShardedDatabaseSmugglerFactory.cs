using System;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Smuggler;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding.Smuggler;

public sealed class ShardedDatabaseSmugglerFactory : AbstractDatabaseSmugglerFactory
{
    private readonly ShardedDocumentDatabase _database;

    public ShardedDatabaseSmugglerFactory([NotNull] ShardedDocumentDatabase database)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    public override DatabaseDestination CreateDestination(CancellationToken token = default)
    {
        return new ShardedDatabaseDestination(_database, token);
    }

    public override DatabaseDestination CreateDestinationForSnapshotRestore(Dictionary<string, SubscriptionState> subscriptions, CancellationToken token = default)
    {
        throw new NotSupportedInShardingException();
    }

    public override DatabaseSource CreateSource(long startDocumentEtag, long startRaftIndex, RavenLogger logger)
    {
        return new ShardedDatabaseSource(_database, startDocumentEtag, startRaftIndex, logger);
    }

    public override SmugglerBase CreateForRestore(
        DatabaseRecord databaseRecord,
        ISmugglerSource source,
        ISmugglerDestination destination,
        JsonOperationContext context,
        DatabaseSmugglerOptionsServerSide options,
        SmugglerResult result = null,
        Action<IOperationProgress> onProgress = null,
        CancellationToken token = default)
    {
        return new ShardedDatabaseSmuggler(source, destination, context, databaseRecord, _database.ServerStore, options, result, onProgress, token);
    }

    public override SmugglerBase Create(
        ISmugglerSource source,
        ISmugglerDestination destination,
        JsonOperationContext context,
        DatabaseSmugglerOptionsServerSide options,
        SmugglerResult result = null,
        Action<IOperationProgress> onProgress = null,
        CancellationToken token = default)
    {
        return new SingleShardDatabaseSmuggler(_database, source, destination, _database.Time, context, options, result, onProgress, token);
    }
}
