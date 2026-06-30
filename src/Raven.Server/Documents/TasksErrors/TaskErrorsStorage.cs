using System;
using System.Collections.Immutable;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.TransactionMerger;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;
using Transaction = Voron.Impl.Transaction;

namespace Raven.Server.Documents.ETL;

public unsafe class TaskErrorsStorage
{
    private const int ErrorsLimitPerTaskErrorType = 500;

    private const int TableSizeInPages = 16;
    private DocumentsContextPool _contextPool;
    private DocumentsTransactionOperationsMerger _txMerger;
    private ImmutableHashSet<string> _tablesCreated = ImmutableHashSet.Create<string>(StringComparer.Ordinal);

    private static readonly Func<ImmutableHashSet<string>, string, ImmutableHashSet<string>> AddTableName = static (tables, name) => tables.Add(name);
    private static readonly Func<ImmutableHashSet<string>, string, ImmutableHashSet<string>> RemoveTableName = static (tables, name) => tables.Remove(name);

    public void Initialize(DocumentsContextPool contextPool, DocumentsTransactionOperationsMerger txMerger)
    {
        _contextPool = contextPool;
        _txMerger = txMerger;
    }

    private Table EnsureProcessErrorsTableCreated(Transaction tx, string taskName, TaskCategory taskCategory)
    {
        var tableName = GetProcessErrorsTableName(taskCategory, taskName);

        if (_tablesCreated.Contains(tableName) == false)
        {
            Schemas.TaskProcessErrors.Current.Create(tx, tableName, TableSizeInPages);
            tx.LowLevelTransaction.OnDispose += _ =>
            {
                if (tx.LowLevelTransaction.Committed == false)
                    return;

                ImmutableInterlocked.Update(ref _tablesCreated, AddTableName, tableName);
            };
        }

        return tx.OpenTable(Schemas.TaskProcessErrors.Current, tableName);
    }

    private static Table GetProcessErrorsTable(Transaction tx, string taskName, TaskCategory taskCategory)
    {
        var tableName = GetProcessErrorsTableName(taskCategory, taskName);
        return tx.OpenTable(Schemas.TaskProcessErrors.Current, tableName);
    }

    private Table EnsureItemErrorsTableCreated(Transaction tx, string taskName, TaskCategory taskCategory)
    {
        var tableName = GetItemErrorsTableName(taskCategory, taskName);

        if (_tablesCreated.Contains(tableName) == false)
        {
            Schemas.TaskItemErrors.Current.Create(tx, tableName, TableSizeInPages);
            tx.LowLevelTransaction.OnDispose += _ =>
            {
                if (tx.LowLevelTransaction.Committed == false)
                    return;

                ImmutableInterlocked.Update(ref _tablesCreated, AddTableName, tableName);
            };
        }

        return tx.OpenTable(Schemas.TaskItemErrors.Current, tableName);
    }

    private static Table GetItemErrorsTable(Transaction tx, string taskName, TaskCategory taskCategory)
    {
        var tableName = GetItemErrorsTableName(taskCategory, taskName);
        return tx.OpenTable(Schemas.TaskItemErrors.Current, tableName);
    }

    internal void DeleteTaskErrorsTablesForTask(string taskName, TaskCategory taskCategory)
    {
        _txMerger.EnqueueSync(new DeleteTaskErrorsTablesForTaskCommand(taskName, taskCategory));
    }

    internal void DeleteTaskErrorsTablesForTask<T>(TransactionOperationContext<T> context, TaskCategory taskCategory, string taskName)
        where T : RavenTransaction
    {
        var processErrorsTableName = GetProcessErrorsTableName(taskCategory, taskName);
        var itemErrorsTableName = GetItemErrorsTableName(taskCategory, taskName);

        var tx = context.Transaction.InnerTransaction;

        tx.DeleteTable(processErrorsTableName);
        tx.DeleteTable(itemErrorsTableName);

        tx.LowLevelTransaction.OnDispose += _ =>
        {
            if (tx.LowLevelTransaction.Committed == false)
                return;

            ImmutableInterlocked.Update(ref _tablesCreated, RemoveTableName, processErrorsTableName);
            ImmutableInterlocked.Update(ref _tablesCreated, RemoveTableName, itemErrorsTableName);
        };
    }

    internal void StoreProcessError(TaskCategory taskCategory, TaskProcessError processError)
    {
        _txMerger.EnqueueSync(new StoreTaskProcessErrorCommand(taskCategory, processError));
    }

    internal void StoreProcessError<T>(TransactionOperationContext<T> context, TaskCategory taskCategory, TaskProcessError processError)
        where T : RavenTransaction
    {
        var table = EnsureProcessErrorsTableCreated(context.Transaction.InnerTransaction, processError.TaskName, taskCategory);

        var createdAtTicks = Bits.SwapBytes(processError.CreatedAt.Ticks);
        var affectedDocumentsCountSwapped = Bits.SwapBytes(processError.AffectedDocumentsCount);
        var stepSwapped = Bits.SwapBytes((long)processError.Step);

        var id = context.GetLazyString(Guid.NewGuid().ToString());
        var error = context.GetLazyString(processError.Error);

        if (table.NumberOfEntries >= ErrorsLimitPerTaskErrorType)
        {
            DeleteOldestProcessErrorOfTask(table);
        }

        using (table.Allocate(out TableValueBuilder tvb))
        {
            tvb.Add(id.Buffer, id.Size);
            tvb.Add((byte*)&createdAtTicks, sizeof(long));
            tvb.Add((byte*)&affectedDocumentsCountSwapped, sizeof(long));
            tvb.Add((byte*)&stepSwapped, sizeof(long));
            tvb.Add(error.Buffer, error.Size);

            table.Insert(tvb);
        }
    }

    internal void StoreItemErrors(TaskCategory taskCategory, string taskName, List<TaskItemError> itemErrors)
    {
        if (itemErrors.Count == 0)
            return;

        _txMerger.EnqueueSync(new StoreTaskItemErrorsCommand(taskCategory, taskName, itemErrors));
    }

    internal void StoreItemErrors<T>(TransactionOperationContext<T> context, TaskCategory taskCategory, string taskName, List<TaskItemError> itemErrors)
        where T : RavenTransaction
    {
        var table = EnsureItemErrorsTableCreated(context.Transaction.InnerTransaction, taskName, taskCategory);

        foreach (var itemError in itemErrors)
        {
            StoreItemError(itemError, table, context);
        }

        DeleteOldestItemErrorsOfTask(table);
    }

    private static void StoreItemError(TaskItemError itemError, Table table, JsonOperationContext context)
    {
        var createdAtTicks = Bits.SwapBytes(itemError.CreatedAt.Ticks);
        var stepSwapped = Bits.SwapBytes((long)itemError.Step);

        var documentId = context.GetLazyString(itemError.DocumentId);
        var error = context.GetLazyString(itemError.Error);

        using (table.Allocate(out TableValueBuilder tvb))
        {
            tvb.Add(documentId.Buffer, documentId.Size);
            tvb.Add((byte*)&createdAtTicks, sizeof(long));
            tvb.Add((byte*)&stepSwapped, sizeof(long));
            tvb.Add(error.Buffer, error.Size);

            table.Set(tvb);
        }
    }

    private static TaskProcessErrorTableValue ReadProcessError(ref TableValueReader reader, string taskName)
    {
        var createdAt = new DateTime(Bits.SwapBytes(*(long*)reader.Read(Schemas.TaskProcessErrors.TaskProcessErrorsTable.CreatedAtIndex, out _)));
        var affectedDocumentsCount = Bits.SwapBytes(*(long*)reader.Read(Schemas.TaskProcessErrors.TaskProcessErrorsTable.AffectedDocumentsCountIndex, out _));
        var step = Bits.SwapBytes(*(long*)reader.Read(Schemas.TaskProcessErrors.TaskProcessErrorsTable.StepIndex, out _));
        var error = reader.ReadString(Schemas.TaskProcessErrors.TaskProcessErrorsTable.ErrorIndex);

        return new TaskProcessErrorTableValue
        {
            CreatedAt = createdAt,
            TaskName = taskName,
            AffectedDocumentsCount = affectedDocumentsCount,
            Step = step,
            Error = error
        };
    }

    private static TaskItemErrorTableValue ReadItemError(ref TableValueReader reader, string taskName)
    {
        var createdAt = new DateTime(Bits.SwapBytes(*(long*)reader.Read(Schemas.TaskItemErrors.TaskItemErrorsTable.CreatedAtIndex, out _)));
        var documentId = reader.ReadString(Schemas.TaskItemErrors.TaskItemErrorsTable.DocumentIdIndex);
        var step = Bits.SwapBytes(*(long*)reader.Read(Schemas.TaskItemErrors.TaskItemErrorsTable.StepIndex, out _));
        var error = reader.ReadString(Schemas.TaskItemErrors.TaskItemErrorsTable.ErrorIndex);

        return new TaskItemErrorTableValue
        {
            CreatedAt = createdAt,
            TaskName = taskName,
            DocumentId = documentId,
            Step = step,
            Error = error
        };
    }

    public List<TaskProcessErrorTableValue> ReadAllProcessErrors(TaskCategory taskCategory)
    {
        var errors = new List<TaskProcessErrorTableValue>();

        using (_contextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            foreach (var taskName in EnumerateStoredTaskNames(taskCategory, context.Transaction.InnerTransaction, Schemas.TaskProcessErrors.TaskProcessErrorsTree))
            {
                var processErrors = ReadProcessErrorsOfTask(context, taskName, taskCategory);
                errors.AddRange(processErrors);
            }
        }

        return errors;
    }

    public List<(string TaskName, List<TaskProcessErrorTableValue> ProcessErrors, List<TaskItemErrorTableValue> ItemErrors)> ReadAllErrorsGroupedByTask(TaskCategory taskCategory)
    {
        using (_contextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var tx = context.Transaction.InnerTransaction;
            var taskNames = new HashSet<string>(StringComparer.Ordinal);

            foreach (var name in EnumerateStoredTaskNames(taskCategory, tx, Schemas.TaskProcessErrors.TaskProcessErrorsTree))
                taskNames.Add(name);
            foreach (var name in EnumerateStoredTaskNames(taskCategory, tx, Schemas.TaskItemErrors.TaskItemErrorsTree))
                taskNames.Add(name);

            var result = new List<(string, List<TaskProcessErrorTableValue>, List<TaskItemErrorTableValue>)>(taskNames.Count);

            foreach (var taskName in taskNames)
            {
                var processErrors = ReadProcessErrorsOfTask(context, taskName, taskCategory).ToList();
                var itemErrors = ReadItemErrorsOfTask(context, taskName, taskCategory).ToList();

                result.Add((taskName, processErrors, itemErrors));
            }

            return result;
        }
    }

    public List<(string TaskName, List<TaskProcessErrorTableValue> ProcessErrors, List<TaskItemErrorTableValue> ItemErrors)> ReadErrorsForTasks(TaskCategory taskCategory, IEnumerable<string> taskNames)
    {
        using (_contextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var result = new List<(string, List<TaskProcessErrorTableValue>, List<TaskItemErrorTableValue>)>();

            foreach (var taskName in taskNames)
            {
                var processErrors = ReadProcessErrorsOfTask(context, taskName, taskCategory).ToList();
                var itemErrors = ReadItemErrorsOfTask(context, taskName, taskCategory).ToList();

                result.Add((taskName, processErrors, itemErrors));
            }

            return result;
        }
    }

    public List<TaskItemErrorTableValue> ReadAllItemErrors(TaskCategory taskCategory)
    {
        var errors = new List<TaskItemErrorTableValue>();

        using (_contextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            foreach (var taskName in EnumerateStoredTaskNames(taskCategory, context.Transaction.InnerTransaction, Schemas.TaskItemErrors.TaskItemErrorsTree))
            {
                var itemErrors = ReadItemErrorsOfTask(context, taskName, taskCategory);
                errors.AddRange(itemErrors);
            }
        }

        return errors;
    }

    public long ReadTotalErrorsCount(TaskCategory taskCategory)
    {
        var errorsCount = 0L;

        using (_contextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var tx = context.Transaction.InnerTransaction;
            var taskNames = new HashSet<string>(StringComparer.Ordinal);

            foreach (var name in EnumerateStoredTaskNames(taskCategory, tx, Schemas.TaskProcessErrors.TaskProcessErrorsTree))
                taskNames.Add(name);
            foreach (var name in EnumerateStoredTaskNames(taskCategory, tx, Schemas.TaskItemErrors.TaskItemErrorsTree))
                taskNames.Add(name);

            foreach (var taskName in taskNames)
                errorsCount += ReadErrorsCountOfTask(context, taskName, taskCategory);
        }

        return errorsCount;
    }

    public long ReadErrorsCountOfTask(TaskCategory taskCategory, string taskName)
    {
        using (_contextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            return ReadErrorsCountOfTask(context, taskName, taskCategory);
        }
    }

    private long ReadErrorsCountOfTask(DocumentsOperationContext context, string taskName, TaskCategory taskCategory)
    {
        var processErrorsCount = ReadProcessErrorsCountOfTask(context, taskName, taskCategory);
        var itemErrorsCount = ReadItemErrorsCountOfTask(context, taskName, taskCategory);

        return processErrorsCount + itemErrorsCount;
    }

    private long ReadProcessErrorsCountOfTask(DocumentsOperationContext context, string taskName, TaskCategory taskCategory)
    {
        var table = GetProcessErrorsTable(context.Transaction.InnerTransaction, taskName, taskCategory);
        if (table == null)
            return 0;

        return table.NumberOfEntries;
    }

    private long ReadItemErrorsCountOfTask(DocumentsOperationContext context, string taskName, TaskCategory taskCategory)
    {
        var table = GetItemErrorsTable(context.Transaction.InnerTransaction, taskName, taskCategory);
        if (table == null)
            return 0;

        return table.NumberOfEntries;
    }

    public List<TaskProcessErrorTableValue> ReadProcessErrorsOfTask(TaskCategory taskCategory, string taskName)
    {
        using (_contextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            return ReadProcessErrorsOfTask(context, taskName, taskCategory).ToList();
        }
    }

    public List<TaskItemErrorTableValue> ReadItemErrorsOfTask(TaskCategory taskCategory, string taskName)
    {
        using (_contextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            return ReadItemErrorsOfTask(context, taskName, taskCategory).ToList();
        }
    }

    private IEnumerable<TaskProcessErrorTableValue> ReadProcessErrorsOfTask(DocumentsOperationContext context, string taskName, TaskCategory taskCategory)
    {
        var table = GetProcessErrorsTable(context.Transaction.InnerTransaction, taskName, taskCategory);
        if (table == null)
            yield break;

        foreach (var tvh in table.SeekForwardFrom(Schemas.TaskProcessErrors.Current.Indexes[Schemas.TaskProcessErrors.ByCreatedAt], Slices.BeforeAllKeys, 0))
        {
            var error = ReadProcessError(ref tvh.Result.Reader, taskName);

            yield return error;
        }
    }

    private IEnumerable<TaskItemErrorTableValue> ReadItemErrorsOfTask(DocumentsOperationContext context, string taskName, TaskCategory taskCategory)
    {
        var table = GetItemErrorsTable(context.Transaction.InnerTransaction, taskName, taskCategory);
        if (table == null)
            yield break;

        foreach (var tvh in table.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
        {
            var error = ReadItemError(ref tvh.Reader, taskName);

            yield return error;
        }
    }

    internal TaskProcessErrorTableValue ReadLatestProcessErrorOfTask(TaskCategory taskCategory, string taskName)
    {
        using (_contextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var table = GetProcessErrorsTable(context.Transaction.InnerTransaction, taskName, taskCategory);

            if (table == null)
                return null;

            var tvh = table.SeekOneBackwardFrom(Schemas.TaskProcessErrors.Current.Indexes[Schemas.TaskProcessErrors.ByCreatedAt], Slices.Empty, Slices.AfterAllKeys);

            if (tvh == null)
                return null;

            return ReadProcessError(ref tvh.Reader, taskName);
        }
    }

    public void DeleteAllErrors()
    {
        using (_contextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            List<(TaskCategory TaskType, string TaskName)> toDelete;

            using (context.OpenReadTransaction())
            {
                var tx = context.Transaction.InnerTransaction;
                var seen = new HashSet<(TaskCategory, string)>();

                foreach (TaskCategory taskType in Enum.GetValues(typeof(TaskCategory)))
                {
                    foreach (var name in EnumerateStoredTaskNames(taskType, tx, Schemas.TaskProcessErrors.TaskProcessErrorsTree))
                        seen.Add((taskType, name));
                    foreach (var name in EnumerateStoredTaskNames(taskType, tx, Schemas.TaskItemErrors.TaskItemErrorsTree))
                        seen.Add((taskType, name));
                }

                toDelete = seen.ToList();
            }

            foreach (var (taskType, taskName) in toDelete)
                DeleteErrorsOfTask(taskName, taskType);
        }
    }

    public void DeleteErrorsOfTask(string taskName, TaskCategory taskCategory)
    {
        DeleteTaskErrorsTablesForTask(taskName, taskCategory);
    }

    private static IEnumerable<string> EnumerateStoredTaskNames(TaskCategory taskCategory, Transaction tx, string tree)
    {
        var prefix = $"{taskCategory}.{tree}.";

        using (Slice.From(tx.Allocator, prefix, out Slice prefixSlice))
        using (var it = tx.LowLevelTransaction.RootObjects.Iterate(prefetch: false))
        {
            it.SetRequiredPrefix(prefixSlice);

            if (it.Seek(prefixSlice) == false)
                yield break;

            do
            {
                var key = it.CurrentKey.ToString();
                yield return key.Substring(prefix.Length);
            }
            while (it.MoveNext());
        }
    }

    private static void DeleteOldestProcessErrorOfTask(Table table)
    {
        if (table == null)
            return;

        table.DeleteForwardFrom(Schemas.TaskProcessErrors.Current.Indexes[Schemas.TaskProcessErrors.ByCreatedAt], Slices.BeforeAllKeys, false, 1);
    }

    private static void DeleteOldestItemErrorsOfTask(Table table)
    {
        if (table == null || table.NumberOfEntries <= ErrorsLimitPerTaskErrorType)
            return;

        var numberOfEntriesToDelete = table.NumberOfEntries - ErrorsLimitPerTaskErrorType;
        table.DeleteForwardFrom(Schemas.TaskItemErrors.Current.Indexes[Schemas.TaskItemErrors.ByCreatedAt], Slices.BeforeAllKeys, false, numberOfEntriesToDelete);
    }

    private static string GetProcessErrorsTableName(TaskCategory taskCategory, string taskName)
    {
        return $"{taskCategory}.{Schemas.TaskProcessErrors.TaskProcessErrorsTree}.{taskName}";
    }

    private static string GetItemErrorsTableName(TaskCategory taskCategory, string taskName)
    {
        return $"{taskCategory}.{Schemas.TaskItemErrors.TaskItemErrorsTree}.{taskName}";
    }
}
