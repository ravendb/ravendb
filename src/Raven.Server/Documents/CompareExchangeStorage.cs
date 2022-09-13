using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

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

    public IEnumerable<(CompareExchangeKey Key, long Index, BlittableJsonReaderObject Value)> GetCompareExchangeFromPrefix(
        ClusterOperationContext context,
        long fromIndex,
        long take)
    {
        return _database.ServerStore.Cluster.GetCompareExchangeFromPrefix(context, _databaseName, fromIndex, take);
    }

    public IEnumerable<(CompareExchangeKey Key, long Index)> GetCompareExchangeTombstonesByKey(
        ClusterOperationContext context,
        long fromIndex = 0,
        long take = long.MaxValue)
    {
        return _database.ServerStore.Cluster.GetCompareExchangeTombstonesByKey(context, _databaseName, fromIndex, take);
    }

    public bool HasCompareExchangeTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(ClusterOperationContext context, long start, long end)
    {
        return _database.ServerStore.Cluster.HasCompareExchangeTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(context, _databaseName, start, end);
    }

    public long GetLastCompareExchangeIndex(ClusterOperationContext context)
    {
        if (_databaseName == null)
            return 0;

        return _database.ServerStore.Cluster.GetLastCompareExchangeIndexForDatabase(context, _databaseName);
    }

    public long GetLastCompareExchangeTombstoneIndex(ClusterOperationContext context)
    {
        if (_databaseName == null)
            return 0;

        return _database.ServerStore.Cluster.GetLastCompareExchangeTombstoneIndexForDatabase(context, _databaseName);
    }

    public bool ShouldHandleChange(CompareExchangeChange change)
    {
        return string.Equals(_databaseName, change.Database, StringComparison.OrdinalIgnoreCase);
    }
}
