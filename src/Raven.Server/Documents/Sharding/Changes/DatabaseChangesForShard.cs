using System;
using Raven.Client.Documents.Changes;
using Raven.Client.Http;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Sharding.Changes;

internal class DatabaseChangesForShard : DatabaseChanges
{
    public DatabaseChangesForShard(ServerStore server, RequestExecutor requestExecutor, string databaseName, Action onDispose, string nodeTag)
        : base(requestExecutor, databaseName, onDispose, nodeTag)
    {
    }
}
