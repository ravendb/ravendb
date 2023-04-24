using System;
using System.Collections.Generic;
using System.Diagnostics;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents;

public abstract class AbstractCompareExchangeStorage
{
    private readonly ServerStore _serverStore;


    private string _databaseName;

    private bool _initialized;

    protected AbstractCompareExchangeStorage([NotNull] ServerStore serverStore)
    {
        _serverStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
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

        return _serverStore.Cluster.GetCompareExchangeFromPrefix(context, _databaseName, fromIndex, take);
    }

    public IEnumerable<(CompareExchangeKey Key, long Index)> GetCompareExchangeTombstonesByKey(
        ClusterOperationContext context,
        long fromIndex = 0,
        long take = long.MaxValue)
    {
        AssertInitialized();

        return _serverStore.Cluster.GetCompareExchangeTombstonesByKey(context, _databaseName, fromIndex, take);
    }

    public bool HasCompareExchangeTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(ClusterOperationContext context, long start, long end)
    {
        AssertInitialized();

        return _serverStore.Cluster.HasCompareExchangeTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(context, _databaseName, start, end);
    }

    public (long Index, BlittableJsonReaderObject Value) GetCompareExchangeValue<TRavenTransaction>(TransactionOperationContext<TRavenTransaction> context, string key)
        where TRavenTransaction : RavenTransaction
    {
        AssertInitialized();

        var prefix = CompareExchangeKey.GetStorageKey(_databaseName, key);
        return _serverStore.Cluster.GetCompareExchangeValue(context, prefix);
    }

    public long GetLastCompareExchangeIndex(ClusterOperationContext context)
    {
        AssertInitialized();

        if (_databaseName == null)
            return 0;

        return _serverStore.Cluster.GetLastCompareExchangeIndexForDatabase(context, _databaseName);
    }

    public long GetLastCompareExchangeTombstoneIndex(ClusterOperationContext context)
    {
        AssertInitialized();

        if (_databaseName == null)
            return 0;

        return _serverStore.Cluster.GetLastCompareExchangeTombstoneIndexForDatabase(context, _databaseName);
    }

    public bool ShouldHandleChange(CompareExchangeChange change)
    {
        AssertInitialized();

        return string.Equals(_databaseName, change.Database, StringComparison.OrdinalIgnoreCase);
    }

    public string GetCompareExchangeStorageKey(string key) => CompareExchangeKey.GetStorageKey(_databaseName, key);

    public IEnumerable<(CompareExchangeKey Key, long Index, BlittableJsonReaderObject Value)> GetCompareExchangeValuesStartsWith(ClusterOperationContext context, string prefix, long start = 0, long pageSize = 1024)
    {
        return _serverStore.Cluster.GetCompareExchangeValuesStartsWith(context, _databaseName, prefix, start, pageSize);
    }

    [Conditional("DEBUG")]
    private void AssertInitialized()
    {
        if (_initialized == false)
            throw new InvalidOperationException($"Database '{_databaseName}' did not initialize the compare exchange storage.");
    }
}
