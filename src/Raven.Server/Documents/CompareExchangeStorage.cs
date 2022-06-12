using System;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents;

public class CompareExchangeStorage
{
    private readonly DocumentDatabase _database;

    private string _databaseName;

    public CompareExchangeStorage([NotNull] DocumentDatabase database)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    public void Initialize([NotNull] string databaseName)
    {
        _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
    }

    public long GetLastCompareExchangeIndex(ClusterOperationContext context)
    {
        return _database.ServerStore.Cluster.GetLastCompareExchangeIndexForDatabase(context, _databaseName);
    }

    public long GetLastCompareExchangeTombstoneIndex(ClusterOperationContext context)
    {
        return _database.ServerStore.Cluster.GetLastCompareExchangeTombstoneIndexForDatabase(context, _databaseName);
    }
}
