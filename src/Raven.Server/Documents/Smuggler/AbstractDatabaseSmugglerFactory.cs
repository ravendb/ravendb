using System;
using System.Threading;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.Sharding;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Smuggler;

public abstract class AbstractDatabaseSmugglerFactory
{
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
        DatabaseSmugglerOptionsServerSide options = null,
        SmugglerResult result = null,
        Action<IOperationProgress> onProgress = null,
        CancellationToken token = default);
}
