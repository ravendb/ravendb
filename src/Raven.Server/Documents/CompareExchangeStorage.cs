using System;
using System.Collections.Generic;
using System.Diagnostics;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents;

public class CompareExchangeStorage
{
    private readonly DocumentDatabase _database;

    private string _databaseName;

    private bool _initialized;

    public CompareExchangeStorage([NotNull] DocumentDatabase database)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    public void Initialize([NotNull] string databaseName)
    {
        _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));

        _initialized = true;
    }

    public IEnumerable<(CompareExchangeKey Key, long Index, BlittableJsonReaderObject Value)> GetCompareExchangeFromPrefix(
        ClusterOperationContext context,
        long fromIndex,
        long take)
    {
        AssertInitialized();

        return _database.ServerStore.Cluster.GetCompareExchangeFromPrefix(context, _databaseName, fromIndex, take);
    }

    public IEnumerable<(CompareExchangeKey Key, long Index)> GetCompareExchangeTombstonesByKey(
        ClusterOperationContext context,
        long fromIndex = 0,
        long take = long.MaxValue)
    {
        AssertInitialized();

        return _database.ServerStore.Cluster.GetCompareExchangeTombstonesByKey(context, _databaseName, fromIndex, take);
    }

    public bool HasCompareExchangeTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(ClusterOperationContext context, long start, long end)
    {
        AssertInitialized();

        return _database.ServerStore.Cluster.HasCompareExchangeTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(context, _databaseName, start, end);
    }

    public long GetLastCompareExchangeIndex(ClusterOperationContext context)
    {
        AssertInitialized();

        if (_databaseName == null)
            return 0;

        return _database.ServerStore.Cluster.GetLastCompareExchangeIndexForDatabase(context, _databaseName);
    }

    public long GetLastCompareExchangeTombstoneIndex(ClusterOperationContext context)
    {
        AssertInitialized();

        if (_databaseName == null)
            return 0;

        return _database.ServerStore.Cluster.GetLastCompareExchangeTombstoneIndexForDatabase(context, _databaseName);
    }

    public bool ShouldHandleChange(CompareExchangeChange change)
    {
        AssertInitialized();

        return string.Equals(_databaseName, change.Database, StringComparison.OrdinalIgnoreCase);
    }

    [Conditional("DEBUG")]
    private void AssertInitialized()
    {
        if (_initialized == false)
            throw new InvalidOperationException($"Database '{_database.Name}' did not initialize the compare exchange storage.");
    }
}
