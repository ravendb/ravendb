using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Logging;
using Sparrow.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Bits = Sparrow.Binary.Bits;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public class BackupHistoryStorage
{
    private const string JsonDocumentId = "backup-history-entry";

    private StorageEnvironment _environment;
    private TransactionContextPool _contextPool;

    private readonly RavenLogger _logger = RavenLogManager.Instance.GetLoggerForServer<BackupHistoryStorage>();

    private static readonly TableSchema BackupHistoryTableSchema = new();
    private static readonly TableSchema BackupResultDetailsTableSchema = new();

    private static readonly Slice ByGroupSlice;

    static BackupHistoryStorage()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
            Slice.From(ctx, "ByGroup", out ByGroupSlice);

        BackupHistoryTableSchema.DefineKey(new TableSchema.IndexDef
        {
            StartIndex = (int)BackupHistorySchema.BackupHistoryColumns.PrimaryKey,
            Count = 1
        });

        BackupHistoryTableSchema.DefineIndex(new TableSchema.IndexDef
        {
            Name = ByGroupSlice,
            StartIndex = (int)BackupHistorySchema.BackupHistoryColumns.GroupKey,
            Count = 1
        });

        BackupResultDetailsTableSchema.DefineKey(new TableSchema.IndexDef
        {
            StartIndex = (int)BackupResultDetailsSchema.BackupResultDetailsColumns.PrimaryKey,
            Count = 1
        });
    }

    public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
    {
        _environment = environment;
        _contextPool = contextPool;

        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var tx = _environment.WriteTransaction(context.PersistentContext))
        {
            BackupHistoryTableSchema.Create(tx, BackupHistorySchema.TableName, 16);
            BackupResultDetailsTableSchema.Create(tx, BackupResultDetailsSchema.TableName, 16);
            tx.Commit();
        }
    }

    public static unsafe void StoreBackupStatus<T>(TransactionOperationContext<T> context, string databaseName, long taskId, BackupHistoryEntry entry) where T : RavenTransaction
    {
        var table = context.Transaction.InnerTransaction.OpenTable(BackupHistoryTableSchema, BackupHistorySchema.TableName);

        using (BackupHistorySchema.GetPrimaryKey(context.Allocator, databaseName, taskId, entry.CreatedAt.Ticks, out var primaryKeySlice))
        using (BackupHistorySchema.GetGroupKey(context.Allocator, databaseName, taskId, entry.GetFullBackupTicks(), entry.BackupKind, out var groupKeySlice))
        using (table.Allocate(out TableValueBuilder tvb))
        using (var json = context.ReadObject(entry.ToJson(), JsonDocumentId))
        {
            tvb.Add(primaryKeySlice);
            tvb.Add(groupKeySlice);
            tvb.Add(json.BasePointer, json.Size);

            table.Set(tvb);
        }
    }

    public static unsafe void StoreBackupResultDetails<T>(TransactionOperationContext<T> context, string databaseName, long taskId, BackupHistoryEntry entry, BackupResult result) where T : RavenTransaction
    {
        var table = context.Transaction.InnerTransaction.OpenTable(BackupResultDetailsTableSchema, BackupResultDetailsSchema.TableName);

        using (BackupHistorySchema.GetPrimaryKey(context.Allocator, databaseName, taskId, entry.CreatedAt.Ticks, out var primaryKeySlice))
        using (var json = context.ReadObject(result.ToJson(), JsonDocumentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
        using (table.Allocate(out TableValueBuilder tvb))
        {
            tvb.Add(primaryKeySlice);
            tvb.Add(json.BasePointer, json.Size);

            table.Set(tvb);
        }
    }

    public static unsafe BlittableJsonReaderObject GetBackupResult(TransactionOperationContext context, string databaseName, long taskId, long createdAtTicks)
    {
        var table = context.Transaction.InnerTransaction.OpenTable(BackupResultDetailsTableSchema, BackupResultDetailsSchema.TableName);

        using (BackupHistorySchema.GetPrimaryKey(context.Allocator, databaseName, taskId, createdAtTicks, out var primaryKeySlice))
        {
            if (table.ReadByKey(primaryKeySlice, out var tvr) == false)
                return null;

            var jsonPointer = tvr.Read((int)BackupResultDetailsSchema.BackupResultDetailsColumns.Data, out var size);
            return new BlittableJsonReaderObject(jsonPointer, size, context);
        }
    }

    internal static void UpdateBackupHistory(string databaseName, ServerStore serverStore, long taskId, BackupHistoryEntry entry, BackupResult backupResult,
        Action<IOperationProgress> onProgress, RavenLogger logger, OperationCancelToken operationCancelToken)
    {
        try
        {
            AsyncHelpers.RunSync(() => UpdateBackupHistoryAsync(serverStore, [new BackupHistoryUpdate(databaseName, taskId, entry, backupResult)], backupResult, onProgress, operationCancelToken));
        }
        catch (Exception e)
        {
            var message = $"Failed to update the backup history of task {taskId} of database '{databaseName}'";
            backupResult?.AddError($"{message}{Environment.NewLine}{e}");
            onProgress?.Invoke(backupResult?.Progress);

            if (logger.IsErrorEnabled)
                logger.Error(message, e);
        }
    }

    internal static Task UpdateBackupHistoryAsync(ServerStore serverStore, IReadOnlyList<BackupHistoryUpdate> updates, BackupResult backupResult = default,
        Action<IOperationProgress> onProgress = default, OperationCancelToken operationCancelToken = default)
    {
        return BackupHelper.RunWithRetriesAsync(maxRetries: 10, async () =>
            {
                serverStore.DatabaseInfoCache.BackupHistoryStorage._forTestingPurposes?.BeforeBackupHistoryUpdate?.Invoke();
                await serverStore.Engine.TxMerger.Enqueue(new UpdateBackupHistoryCommand(updates));
                return default;
            },
            infoMessage: "Updating the backup history",
            errorMessage: "Failed to update the backup history",
            backupResult, onProgress, operationCancelToken);
    }

    internal static int DeleteExpiredEntries(ClusterOperationContext context, string databaseName, DateTime cutoffTime, HashSet<long> tasksToKeepNewestFullBackup, int maxEntriesToDelete)
    {
        var historyTable = context.Transaction.InnerTransaction.OpenTable(BackupHistoryTableSchema, BackupHistorySchema.TableName);
        var detailsTable = context.Transaction.InnerTransaction.OpenTable(BackupResultDetailsTableSchema, BackupResultDetailsSchema.TableName);

        var expiredEntries = new List<(long TaskId, long CreatedAtTicks)>();
        foreach (var taskId in GetTaskIds(context.Allocator, historyTable, databaseName))
        {
            var keptFullBackupTicks = tasksToKeepNewestFullBackup.Contains(taskId)
                ? GetNewestSuccessfulFullBackupTicks(context, historyTable, databaseName, taskId)
                : null;

            CollectExpiredEntries(context.Allocator, historyTable, databaseName, taskId, cutoffTime.Ticks, keptFullBackupTicks, maxEntriesToDelete, expiredEntries);
            if (expiredEntries.Count == maxEntriesToDelete)
                break;
        }

        foreach (var (taskId, createdAtTicks) in expiredEntries)
        {
            using (BackupHistorySchema.GetPrimaryKey(context.Allocator, databaseName, taskId, createdAtTicks, out var primaryKeySlice))
            {
                historyTable.DeleteByKey(primaryKeySlice);
                detailsTable.DeleteByKey(primaryKeySlice);
            }
        }

        return expiredEntries.Count;
    }

    internal static bool HasExpiredEntries(TransactionOperationContext context, string databaseName, DateTime cutoffTime)
    {
        var table = context.Transaction.InnerTransaction.OpenTable(BackupHistoryTableSchema, BackupHistorySchema.TableName);

        foreach (var taskId in GetTaskIds(context.Allocator, table, databaseName))
        {
            using (BackupHistorySchema.GetTaskKeyPrefix(context.Allocator, databaseName, taskId, out var taskPrefixSlice))
            {
                foreach (var (key, _) in table.SeekByPrimaryKeyPrefix(taskPrefixSlice, Slices.Empty, skip: 0))
                {
                    if (BackupHistorySchema.ReadValue(key, positionFromEnd: 0) < cutoffTime.Ticks)
                        return true;

                    break;
                }
            }
        }

        return false;
    }

    internal static unsafe List<string> GetDatabaseNames(TransactionOperationContext context)
    {
        var table = context.Transaction.InnerTransaction.OpenTable(BackupHistoryTableSchema, BackupHistorySchema.TableName);
        var databaseNames = new List<string>();

        var startAfter = Slices.Empty;
        using (BackupHistorySchema.GetBackupsPrefix(context.Allocator, out var backupsPrefixSlice))
        {
            while (true)
            {
                string databaseName = null;
                foreach (var (key, _) in table.SeekByPrimaryKeyPrefix(backupsPrefixSlice, startAfter, skip: 0))
                {
                    databaseName = BackupHistorySchema.ReadDatabaseName(key);
                    break;
                }

                if (databaseName == null)
                    return databaseNames;

                databaseNames.Add(databaseName);

                using (BackupHistorySchema.GetDatabaseKeyPrefix(context.Allocator, databaseName, out var databasePrefixSlice))
                {
                    databasePrefixSlice.Content.Ptr[databasePrefixSlice.Size - 1]++;
                    startAfter = databasePrefixSlice.Clone(context.Allocator);
                }
            }
        }
    }

    public static BlittableJsonReaderObject GetBackupHistory(TransactionOperationContext context,
        RawDatabaseRecord databaseRecord,
        int? shardNumber,
        bool includeIncrementals,
        long? requestedTaskId = null,
        long? requestedFullBackupCreatedAtTicks = null)
    {
        var backupHistory = new BackupHistory(databaseRecord.DatabaseName);
        var table = context.Transaction.InnerTransaction.OpenTable(BackupHistoryTableSchema, BackupHistorySchema.TableName);

        foreach (var source in GetHistorySources(databaseRecord, shardNumber))
        {
            var taskIds = requestedTaskId.HasValue
                ? [requestedTaskId.Value]
                : GetTaskIds(context.Allocator, table, source.DatabaseName);

            foreach (var taskId in taskIds)
            {
                var entries = requestedFullBackupCreatedAtTicks.HasValue
                    ? ReadGroupEntries(context, table, source.DatabaseName, taskId, requestedFullBackupCreatedAtTicks.Value)
                    : ReadTaskEntries(context, table, source.DatabaseName, taskId);

                foreach (var entry in entries)
                    backupHistory.Add(entry, taskId, source.ShardNumber, includeIncrementals);
            }
        }

        backupHistory.UpdateTaskNames(databaseRecord);
        return context.ReadObject(backupHistory.ToJson(), nameof(BackupHistory));
    }

    private static IEnumerable<HistorySource> GetHistorySources(RawDatabaseRecord databaseRecord, int? shardNumber)
    {
        if (databaseRecord.IsSharded == false)
            return [new HistorySource(databaseRecord.DatabaseName, ShardNumber: null)];

        IEnumerable<int> shardNumbers = shardNumber.HasValue ? [shardNumber.Value] : databaseRecord.Sharding.Shards.Keys;
        return shardNumbers.Select(shard => new HistorySource(ShardHelper.ToShardName(databaseRecord.DatabaseName, shard), shard));
    }

    private readonly record struct HistorySource(string DatabaseName, int? ShardNumber);

    private static IEnumerable<BackupHistoryEntry> ReadTaskEntries(TransactionOperationContext context, Table table, string databaseName, long taskId)
    {
        using (BackupHistorySchema.GetTaskKeyPrefix(context.Allocator, databaseName, taskId, out var taskPrefixSlice))
        {
            foreach (var (_, result) in table.SeekByPrimaryKeyPrefix(taskPrefixSlice, Slices.Empty, skip: 0))
                yield return GetBackupEntry(context, result.Reader);
        }
    }

    private static IEnumerable<BackupHistoryEntry> ReadGroupEntries(TransactionOperationContext context, Table table, string databaseName, long taskId, long fullBackupTicks)
    {
        using (BackupHistorySchema.GetGroupKeyPrefix(context.Allocator, databaseName, taskId, fullBackupTicks, out var groupPrefixSlice))
        {
            foreach (var result in table.SeekForwardFromPrefix(BackupHistoryTableSchema.Indexes[ByGroupSlice], groupPrefixSlice, groupPrefixSlice, skip: 0))
                yield return GetBackupEntry(context, result.Result.Reader);
        }
    }

    private static List<long> GetTaskIds(ByteStringContext allocator, Table table, string databaseName)
    {
        var taskIds = new List<long>();

        using (BackupHistorySchema.GetDatabaseKeyPrefix(allocator, databaseName, out var databasePrefixSlice))
        {
            var fromTaskId = 0L;
            while (TryGetNextTaskId(allocator, table, databasePrefixSlice, databaseName, fromTaskId, out var taskId))
            {
                taskIds.Add(taskId);
                fromTaskId = taskId + 1;
            }
        }

        return taskIds;
    }

    private static bool TryGetNextTaskId(ByteStringContext allocator, Table table, Slice databasePrefixSlice, string databaseName, long fromTaskId, out long taskId)
    {
        using (BackupHistorySchema.GetTaskKeyPrefix(allocator, databaseName, fromTaskId, out var fromTaskSlice))
        {
            foreach (var (key, _) in table.SeekByPrimaryKeyPrefix(databasePrefixSlice, fromTaskSlice, skip: 0))
            {
                taskId = BackupHistorySchema.ReadValue(key, positionFromEnd: 1);
                return true;
            }
        }

        taskId = default;
        return false;
    }

    private static void CollectExpiredEntries(ByteStringContext allocator, Table table, string databaseName, long taskId, long cutoffTicks, long? keptFullBackupTicks,
        int maxEntriesToDelete, List<(long TaskId, long CreatedAtTicks)> expiredEntries)
    {
        using (BackupHistorySchema.GetTaskKeyPrefix(allocator, databaseName, taskId, out var taskPrefixSlice))
        {
            foreach (var (key, _) in table.SeekByPrimaryKeyPrefix(taskPrefixSlice, Slices.Empty, skip: 0))
            {
                var createdAtTicks = BackupHistorySchema.ReadValue(key, positionFromEnd: 0);
                if (createdAtTicks >= cutoffTicks)
                    return;

                if (createdAtTicks == keptFullBackupTicks)
                    continue;

                expiredEntries.Add((taskId, createdAtTicks));
                if (expiredEntries.Count == maxEntriesToDelete)
                    return;
            }
        }
    }

    private static unsafe long? GetNewestSuccessfulFullBackupTicks<T>(TransactionOperationContext<T> context, Table table, string databaseName, long taskId) where T : RavenTransaction
    {
        var index = BackupHistoryTableSchema.Indexes[ByGroupSlice];

        using (BackupHistorySchema.GetTaskKeyPrefix(context.Allocator, databaseName, taskId, out var taskPrefixSlice))
        {
            var fullBackupTicks = long.MaxValue;
            var backupKind = BackupKind.Full;

            while (true)
            {
                Table.TableValueHolder result;
                using (BackupHistorySchema.GetGroupKey(context.Allocator, databaseName, taskId, fullBackupTicks, backupKind, out var lastSlice))
                    result = table.SeekOneBackwardFrom(index, taskPrefixSlice, lastSlice);

                if (result == null)
                    return null;

                var groupKeyPtr = result.Reader.Read((int)BackupHistorySchema.BackupHistoryColumns.GroupKey, out var groupKeySize);
                using (Slice.External(context.Allocator, groupKeyPtr, groupKeySize, out var groupKeySlice))
                {
                    fullBackupTicks = BackupHistorySchema.ReadValue(groupKeySlice, positionFromEnd: 1);

                    if ((BackupKind)BackupHistorySchema.ReadValue(groupKeySlice, positionFromEnd: 0) == BackupKind.Incremental)
                    {
                        backupKind = BackupKind.Full;
                        continue;
                    }
                }

                using (var json = new BlittableJsonReaderObject(result.Reader.Read((int)BackupHistorySchema.BackupHistoryColumns.Data, out var size), size, context))
                {
                    if (json.TryGet(nameof(BackupHistoryEntry.Error), out string error) && error == null)
                        return fullBackupTicks;
                }

                backupKind = BackupKind.None;
            }
        }
    }

    private static unsafe BackupHistoryEntry GetBackupEntry(TransactionOperationContext context, TableValueReader result)
    {
        var ptr = result.Read((int)BackupHistorySchema.BackupHistoryColumns.Data, out var size);
        var json = new BlittableJsonReaderObject(ptr, size, context);
        Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, json);
        return JsonDeserializationServer.BackupHistoryEntry(json);
    }

    private static long GetBackupHistoryEntriesNumber(TransactionOperationContext context)
    {
        using var tx = context.OpenReadTransaction();
        var table = tx.InnerTransaction.OpenTable(BackupHistoryTableSchema, BackupHistorySchema.TableName);

        return table.NumberOfEntries;
    }

    private static long GetBackupResultDetailsNumber(TransactionOperationContext context)
    {
        using var tx = context.OpenReadTransaction();
        var table = tx.InnerTransaction.OpenTable(BackupResultDetailsTableSchema, BackupResultDetailsSchema.TableName);

        return table.NumberOfEntries;
    }

    private static class BackupHistorySchema
    {
        public const string TableName = "BackupHistoryTable";
        private const string BackupsPrefix = "backups/";
        private const byte Separator = (byte)'/';

        public enum BackupHistoryColumns
        {
            PrimaryKey = 0,
            GroupKey = 1,
            Data = 2
        }

        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetBackupsPrefix(ByteStringContext allocator, out Slice keySlice) =>
            Slice.From(allocator, BackupsPrefix, out keySlice);

        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetDatabaseKeyPrefix(ByteStringContext allocator, string databaseName, out Slice keySlice) =>
            GetKey(allocator, databaseName, [], endsWithSeparator: true, out keySlice);

        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetTaskKeyPrefix(ByteStringContext allocator, string databaseName, long taskId, out Slice keySlice) =>
            GetKey(allocator, databaseName, [taskId], endsWithSeparator: false, out keySlice);

        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetPrimaryKey(ByteStringContext allocator, string databaseName, long taskId, long createdAtTicks, out Slice keySlice) =>
            GetKey(allocator, databaseName, [taskId, createdAtTicks], endsWithSeparator: false, out keySlice);

        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetGroupKeyPrefix(ByteStringContext allocator, string databaseName, long taskId, long fullBackupTicks, out Slice keySlice) =>
            GetKey(allocator, databaseName, [taskId, fullBackupTicks], endsWithSeparator: false, out keySlice);

        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetGroupKey(ByteStringContext allocator, string databaseName, long taskId, long fullBackupTicks, BackupKind backupKind, out Slice keySlice) =>
            GetKey(allocator, databaseName, [taskId, fullBackupTicks, (long)backupKind], endsWithSeparator: false, out keySlice);

        public static unsafe long ReadValue(Slice key, int positionFromEnd) =>
            Bits.SwapBytes(*(long*)(key.Content.Ptr + key.Size - (positionFromEnd + 1) * (sizeof(long) + 1) + 1));

        public static unsafe string ReadDatabaseName(Slice key)
        {
            var nameStart = key.Content.Ptr + BackupsPrefix.Length;
            var nameLength = 0;
            while (nameStart[nameLength] != Separator)
                nameLength++;

            return Encoding.UTF8.GetString(nameStart, nameLength);
        }

        private static unsafe ByteStringContext<ByteStringMemoryCache>.InternalScope GetKey(ByteStringContext allocator, string databaseName, ReadOnlySpan<long> values, bool endsWithSeparator,
            out Slice keySlice)
        {
            var name = databaseName.ToLowerInvariant();
            var nameLength = Encoding.UTF8.GetByteCount(name);
            var totalLength = BackupsPrefix.Length + nameLength + values.Length * (1 + sizeof(long)) + (endsWithSeparator ? 1 : 0);

            var scope = allocator.Allocate(totalLength, out var buffer);
            var pos = buffer.Ptr;

            foreach (var ch in BackupsPrefix)
                *pos++ = (byte)ch;

            pos += Encoding.UTF8.GetBytes(name, new Span<byte>(pos, nameLength));

            foreach (var value in values)
            {
                *pos++ = Separator;
                *(long*)pos = Bits.SwapBytes(value);
                pos += sizeof(long);
            }

            if (endsWithSeparator)
                *pos = Separator;

            keySlice = new Slice(buffer);
            return scope;
        }
    }

    private static class BackupResultDetailsSchema
    {
        public const string TableName = "BackupResultDetailsTable";

        public enum BackupResultDetailsColumns
        {
            PrimaryKey = 0,
            Data = 1
        }
    }

    internal TestingStuff _forTestingPurposes;

    internal TestingStuff ForTestingPurposesOnly()
    {
        if (_forTestingPurposes != null)
            return _forTestingPurposes;

        return _forTestingPurposes = new TestingStuff();
    }

    internal class TestingStuff
    {
        internal Action BeforeBackupHistoryUpdate { get; set; }
        internal long BackupHistoryEntriesNumber(TransactionOperationContext context) => GetBackupHistoryEntriesNumber(context);
        internal long BackupResultDetailsNumber(TransactionOperationContext context) => GetBackupResultDetailsNumber(context);
    }
}
