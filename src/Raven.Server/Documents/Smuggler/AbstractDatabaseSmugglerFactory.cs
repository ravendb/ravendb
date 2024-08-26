using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Smuggler;

public abstract class AbstractDatabaseSmugglerFactory
{
    public abstract DatabaseDestination CreateDestination(CancellationToken token = default);

    public abstract DatabaseDestination CreateDestinationForSnapshotRestore(Dictionary<string, SubscriptionState> subscriptions, CancellationToken token = default);

    public abstract DatabaseSource CreateSource(long startDocumentEtag, long startRaftIndex, RavenLogger logger);

    public abstract SmugglerBase CreateForRestore(
        DatabaseRecord databaseRecord,
        ISmugglerSource source,
        ISmugglerDestination destination,
        JsonOperationContext context,
        DatabaseSmugglerOptionsServerSide options,
        SmugglerResult result = null,
        Action<IOperationProgress> onProgress = null,
        CancellationToken token = default);

    public abstract SmugglerBase Create(
        ISmugglerSource source,
        ISmugglerDestination destination,
        JsonOperationContext context,
        DatabaseSmugglerOptionsServerSide options,
        SmugglerResult result = null,
        Action<IOperationProgress> onProgress = null,
        CancellationToken token = default);
}
