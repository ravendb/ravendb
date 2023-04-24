using System;
using System.Collections.Generic;
using System.Diagnostics;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;

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

#pragma warning disable CS0618
        return _serverStore.Cluster.GetCompareExchangeFromPrefix(context, _databaseName, fromIndex, take);
#pragma warning restore CS0618
    }

    public IEnumerable<(CompareExchangeKey Key, long Index)> GetCompareExchangeTombstonesByKey(
        ClusterOperationContext context,
        long fromIndex = 0,
        long take = long.MaxValue)
    {
        AssertInitialized();

#pragma warning disable CS0618
        return _serverStore.Cluster.GetCompareExchangeTombstonesByKey(context, _databaseName, fromIndex, take);
#pragma warning restore CS0618
    }

    public bool HasCompareExchangeTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(ClusterOperationContext context, long start, long end)
    {
        AssertInitialized();

        if (start >= end)
            return false;

        var table = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.CompareExchangeTombstoneSchema, ClusterStateMachine.CompareExchangeTombstones);
        if (table == null)
            return false;

        using (CompareExchangeCommandBase.GetPrefixIndexSlices(context.Allocator, _databaseName, start + 1, out var buffer)) // start + 1 => we want greater than
        using (Slice.External(context.Allocator, buffer, buffer.Length, out var keySlice))
        using (Slice.External(context.Allocator, buffer, buffer.Length - sizeof(long), out var prefix))
        {
            foreach (var tvr in table.SeekForwardFromPrefix(ClusterStateMachine.CompareExchangeTombstoneSchema.Indexes[ClusterStateMachine.CompareExchangeTombstoneIndex], keySlice, prefix, 0))
            {
                var index = ClusterStateMachine.ReadCompareExchangeOrTombstoneIndex(tvr.Result.Reader);
                if (index <= end)
                    return true;

                return false;
            }
        }

        return false;
    }

    public (long Index, BlittableJsonReaderObject Value) GetCompareExchangeValue<TRavenTransaction>(TransactionOperationContext<TRavenTransaction> context, string key)
        where TRavenTransaction : RavenTransaction
    {
        AssertInitialized();

        var prefix = CompareExchangeKey.GetStorageKey(_databaseName, key);

        using (Slice.From(context.Allocator, prefix, out Slice keySlice))
#pragma warning disable CS0618
            return _serverStore.Cluster.GetCompareExchangeValue(context, keySlice);
#pragma warning restore CS0618
    }

    public long GetLastCompareExchangeIndex(ClusterOperationContext context)
    {
        AssertInitialized();

        if (_databaseName == null)
            return 0;

#pragma warning disable CS0618
        return _serverStore.Cluster.GetLastCompareExchangeIndexForDatabase(context, _databaseName);
#pragma warning restore CS0618
    }

    public long GetLastCompareExchangeTombstoneIndex(ClusterOperationContext context)
    {
        AssertInitialized();

        if (_databaseName == null)
            return 0;

        CompareExchangeCommandBase.GetDbPrefixAndLastSlices(context.Allocator, _databaseName, out var prefix, out var last);

        using (prefix.Scope)
        using (last.Scope)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.CompareExchangeTombstoneSchema, ClusterStateMachine.CompareExchangeTombstones);

            var tvh = table.SeekOneBackwardFrom(ClusterStateMachine.CompareExchangeTombstoneSchema.Indexes[ClusterStateMachine.CompareExchangeTombstoneIndex], prefix.Slice, last.Slice);

            if (tvh == null)
                return 0;

            return ClusterStateMachine.ReadCompareExchangeOrTombstoneIndex(tvh.Reader);
        }
    }

    public bool ShouldHandleChange(CompareExchangeChange change)
    {
        AssertInitialized();

        return string.Equals(_databaseName, change.Database, StringComparison.OrdinalIgnoreCase);
    }

    public string GetCompareExchangeStorageKey(string key)
    {
        AssertInitialized();

        return CompareExchangeKey.GetStorageKey(_databaseName, key);
    }

    public IEnumerable<(CompareExchangeKey Key, long Index, BlittableJsonReaderObject Value)> GetCompareExchangeValuesStartsWith(ClusterOperationContext context, string prefix, long start = 0, long pageSize = 1024)
    {
        AssertInitialized();

        prefix = CompareExchangeKey.GetStorageKey(_databaseName, prefix);

        var items = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.CompareExchangeSchema, ClusterStateMachine.CompareExchange);
        using (Slice.From(context.Allocator, prefix, out Slice keySlice))
        {
            foreach (var item in items.SeekByPrimaryKeyPrefix(keySlice, Slices.Empty, start))
            {
                pageSize--;
                var key = ClusterStateMachine.ReadCompareExchangeKey(context, item.Value.Reader, _databaseName);
                var index = ClusterStateMachine.ReadCompareExchangeOrTombstoneIndex(item.Value.Reader);
                var value = ClusterStateMachine.ReadCompareExchangeValue(context, item.Value.Reader);
                yield return (key, index, value);

                if (pageSize == 0)
                    yield break;
            }
        }
    }

    [Conditional("DEBUG")]
    private void AssertInitialized()
    {
        if (_initialized == false)
            throw new InvalidOperationException($"Database '{_databaseName}' did not initialize the compare exchange storage.");
    }
}
