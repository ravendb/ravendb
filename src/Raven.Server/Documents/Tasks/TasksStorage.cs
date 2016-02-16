using System;
using System.Net;

using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

using Voron.Data.Tables;

namespace Raven.Server.Documents.Tasks
{
    public class TasksStorage : IDisposable
    {
        private readonly TableSchema _tasksSchema = new TableSchema();

        public TasksStorage()
        {
            _tasksSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });

            _tasksSchema.DefineFixedSizeIndex("IndexIds", new TableSchema.FixedSizeSchemaIndexDef { StartIndex = 0 });
        }

        public unsafe DocumentsTask GetMergedTask(DocumentsOperationContext context, int indexId, DocumentsTask.DocumentsTaskType type)
        {
            DocumentsTask task = null;
            var totalKeysToProcess = 0;
            var tableName = GetTaskTableName(type);
            var table = new Table(_tasksSchema, tableName, context.Transaction.InnerTransaction);
            foreach (var tvr in table.SeekForwardFrom(_tasksSchema.FixedSizeIndexes["IndexIds"], indexId))
            {
                if (totalKeysToProcess >= 5 * 1024)
                    break;

                int size;
                var ptr = tvr.Read(0, out size);
                var id = IPAddress.NetworkToHostOrder(*(int*)ptr);

                if (id != indexId)
                    break;

                DocumentsTask currentTask;
                try
                {
                    currentTask = DocumentsTask.ToTask(new BlittableJsonReaderObject(tvr.Read(2, out size), size, context));
                }
                catch (Exception)
                {
                    // TODO [ppekrol] log
                    continue;
                }

                totalKeysToProcess += currentTask.NumberOfKeys;

                if (task != null)
                    task.Merge(currentTask);
                else
                    task = currentTask;

                table.Delete(tvr.Id);
            }

            return task;
        }

        public unsafe void AddTask(DocumentsOperationContext context, DocumentsTask task, DateTime addedAt)
        {
            var tableName = GetTaskTableName(task.Type);
            _tasksSchema.Create(context.Transaction.InnerTransaction, tableName);

            var table = new Table(_tasksSchema, tableName, context.Transaction.InnerTransaction);

            var addedAtAsBinary = addedAt.ToBinary();
            var taskAsJson = task.ToJson(context);
            var indexIdBigEndian = IPAddress.HostToNetworkOrder(task.IndexId);

            var tbv = new TableValueBuilder
            {
                { (byte*)&indexIdBigEndian, sizeof(int) },
                { (byte*)&addedAtAsBinary, sizeof(long) },
                { taskAsJson.BasePointer, taskAsJson.Size }
            };

            table.Insert(tbv);
        }

        public void Dispose()
        {
        }

        private static string GetTaskTableName(DocumentsTask.DocumentsTaskType type)
        {
            return "@" + type.ToString().ToLowerInvariant();
        }
    }
}