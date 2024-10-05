using System;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace Raven.Server.Documents.Smuggler;

public sealed class DatabaseSmugglerFactory : AbstractDatabaseSmugglerFactory
{
    private readonly DocumentDatabase _database;

    public DatabaseSmugglerFactory([NotNull] DocumentDatabase database)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    public override DatabaseDestination CreateDestination(CancellationToken token = default)
    {
        return new DatabaseDestination(_database, token);
    }

    public override DatabaseDestination CreateDestinationForSnapshotRestore(Dictionary<string, SubscriptionState> subscriptions, CancellationToken token = default)
    {
        return new SnapshotDatabaseDestination(_database, subscriptions, token);
    }

    public override DatabaseSource CreateSource(long startDocumentEtag, long startRaftIndex, RavenLogger logger)
    {
        return new DatabaseSource(_database, startDocumentEtag, startRaftIndex, logger);
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
        return new DatabaseSmuggler(_database.Name, _database, source, destination, _database.Time, context, options, result, onProgress, token);
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
        return new DatabaseSmuggler(_database.Name, _database, source, destination, _database.Time, context, options, result, onProgress, token);
    }
}
