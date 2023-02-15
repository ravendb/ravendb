using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Json.Serialization;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public unsafe class BackupHistoryStorage
{
    private readonly string _databaseName;
    private StorageEnvironment _environment;
    private TransactionContextPool _contextPool;

    protected readonly Logger Logger;

    private readonly TableSchema _backupHistorySchema = new();

    public BackupHistoryStorage(string databaseName)
    {
        _databaseName = databaseName;
        Logger = LoggingSource.Instance.GetLogger<BackupHistoryStorage>("Server");

        _backupHistorySchema.DefineKey(new TableSchema.SchemaIndexDef
        {
            StartIndex = BackupHistorySchema.BackupHistoryTable.ItemKey,
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
            _backupHistorySchema.Create(tx, BackupHistorySchema.BackupHistory, 16);
            tx.Commit();
        }
    }

    public void StoreBackupHistoryEntries(List<BackupHistoryEntry> entriesToStore)
    {
        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            foreach (var entry in entriesToStore)
            {
                var key = BackupHistoryTableValue.GenerateKey(entry);

                if (Logger.IsInfoEnabled)
                    Logger.Info($"Saving to {nameof(BackupHistoryStorage)} {nameof(BackupHistoryItemType.HistoryEntry)} with `{nameof(BackupHistoryTableValue.Key)}`: `{key}`.");

                StoreInternal(key, entry.ToJson(), tx, context);
            }

            tx.Commit();
        }
    }

    public void StoreBackupDetails(BackupResult result, PeriodicBackupStatus status)
    {
        var key = BackupHistoryTableValue.GenerateKey(_databaseName, status, BackupHistoryItemType.Details);

        if (Logger.IsInfoEnabled)
            Logger.Info($"Saving to {nameof(BackupHistoryStorage)} {nameof(BackupHistoryItemType.Details)} with `{nameof(BackupHistoryTableValue.Key)}`: `{key}`.");

        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var transaction = context.OpenWriteTransaction())
        {
            StoreInternal(key, result.ToJson(), transaction, context);
            transaction.Commit();
        }
    }

    private void StoreInternal(string key, DynamicJsonValue value, RavenTransaction transaction, TransactionOperationContext context)
    {
        var table = transaction.InnerTransaction.OpenTable(_backupHistorySchema, BackupHistorySchema.BackupHistory);

        var lsKey = context.GetLazyString(key);
        using (var json = context.ReadObject(value, nameof(BackupHistorySchema.BackupHistory), BlittableJsonDocumentBuilder.UsageMode.ToDisk))
        using (table.Allocate(out TableValueBuilder tvb))
        {
            tvb.Add(lsKey.Buffer, lsKey.Size);
            tvb.Add(json.BasePointer, json.Size);

            table.Set(tvb);
        }
    }
    
    public Dictionary<string, BlittableJsonReaderObject> ReadItems(TransactionOperationContext context, BackupHistoryItemType type)
    {
        var prefix = $"values/{_databaseName}/backup-history/{type}/";

        using (Slice.From(context.Allocator, prefix, out Slice slice))
        {
            var items = context.Transaction.InnerTransaction.OpenTable(_backupHistorySchema, BackupHistorySchema.BackupHistory);

            return items.SeekByPrimaryKeyPrefix(slice, Slices.Empty, 0)
                .Select(result =>
                {
                    var ptr = result.Value.Reader.Read(1, out int size);
                    var value = new BlittableJsonReaderObject(ptr, size, context);
                    var key = Encoding.UTF8.GetString(result.Value.Reader.Read(0, out size), size);

                    return (key, value);
                }).ToDictionary(x => x.key, x => x.value);
        }
    }

    public List<BackupHistoryEntry> RetractTemporaryStoredBackupHistoryEntries()
    {
        List<BackupHistoryEntry> temporaryStoredEntries = new();
        var prefix = $"values/{_databaseName}/backup-history/{BackupHistoryItemType.HistoryEntry}/";

        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        using (Slice.From(context.Allocator, prefix, out Slice loweredPrefix))
        {
            var items = context.Transaction.InnerTransaction.OpenTable(_backupHistorySchema, BackupHistorySchema.BackupHistory);

            items.DeleteByPrimaryKeyPrefix(loweredPrefix, beforeDelete: value =>
            {
                var ptr = value.Reader.Read(1, out int size);
                var doc = new BlittableJsonReaderObject(ptr, size, context);
                temporaryStoredEntries.Add(JsonDeserializationClient.BackupHistoryEntry(doc));
            });
            tx.Commit();
        }

        return temporaryStoredEntries;
    }

    public BlittableJsonReaderObject GetBackupHistoryDetails(string key, TransactionOperationContext context)
    {
        using var tx = context.OpenReadTransaction();
        var table = tx.InnerTransaction.OpenTable(_backupHistorySchema, BackupHistorySchema.BackupHistory);
        using (Slice.From(tx.InnerTransaction.Allocator, key, out Slice slice))
        {
            if (table.ReadByKey(slice, out TableValueReader tvr) == false)
                return null;

            var jsonPointer = tvr.Read(BackupHistorySchema.BackupHistoryTable.ItemJsonIndex, out var size);
            var json = new BlittableJsonReaderObject(jsonPointer, size, context);

            return json;
        }
    }

    public void HandleDatabaseValueChanged(string type, object changeState)
    {
        if (type != nameof(UpdatePeriodicBackupStatusCommand) || changeState == null)
            return;

        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var transaction = context.OpenWriteTransaction())
        {
            var idsToDelete = (List<string>)changeState;
            idsToDelete.ForEach(id =>
            {
                bool deleteResult;
                var table = transaction.InnerTransaction.OpenTable(_backupHistorySchema, BackupHistorySchema.BackupHistory);

                using (Slice.From(transaction.InnerTransaction.Allocator, id, out Slice alertSlice))
                    deleteResult = table.DeleteByKey(alertSlice);

                if (deleteResult && Logger.IsInfoEnabled)
                    Logger.Info($"Deleted {nameof(BackupHistoryEntry)} '{id}'.");
            });
            transaction.Commit();
        }
    }

    public static class BackupHistorySchema
    {
        public const string BackupHistory = nameof(BackupHistoryTable);

        public static class BackupHistoryTable
        {
            // key structure: values/<databaseName>/backup-history/<BackupHistoryItemType>/<taskId>/<nodeTag>/<createdAtTicksAsId>
            public const int ItemKey = 0;
            public const int ItemJsonIndex = 1;
        }
    }
}

public class BackupHistoryTableValue : IDynamicJsonValueConvertible
{
    public string Key;
    public BlittableJsonReaderObject Value;

    public static string GenerateKey(BackupHistoryEntry entry)
    {
        return GenerateKey(entry.DatabaseName, entry.TaskId, entry.NodeTag, entry.CreatedAt, BackupHistoryItemType.HistoryEntry);
    }

    public static string GenerateKey(string databaseName, PeriodicBackupStatus status, BackupHistoryItemType type)
    {
        var createdAt = status.IsFull
            ? status.LastFullBackup ?? status.Error.At
            : status.LastIncrementalBackup ?? status.Error.At;

        return GenerateKey(databaseName, status.TaskId, status.NodeTag, createdAt, type);
    }

    public static string GenerateKey(string databaseName, long taskId, string nodeTag, DateTime createdAt, BackupHistoryItemType type) => 
        $"values/{databaseName}/backup-history/{type}/{taskId}/{nodeTag}/{createdAt.Ticks}";

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Key)] = Key,
            [nameof(Value)] = Value
        };
    }
}

public enum BackupHistoryItemType
{
    Details,
    HistoryEntry
}
