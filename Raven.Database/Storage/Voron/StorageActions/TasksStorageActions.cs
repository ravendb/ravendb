// -----------------------------------------------------------------------
//  <copyright file="TasksStorageActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Storage.Voron.StorageActions.StructureSchemas;
using System.Linq;

namespace Raven.Database.Storage.Voron.StorageActions
{
    using System;
    using Abstractions.Data;
    using Abstractions.Logging;
    using Database.Impl;
    using Impl;
    using Tasks;
    using global::Voron;
    using global::Voron.Impl;

    internal class TasksStorageActions : StorageActionsBase, ITasksStorageActions
    {
        private static readonly ILog Logger = LogManager.GetCurrentClassLogger();

        private readonly TableStorage tableStorage;

        private readonly IUuidGenerator generator;

        private readonly Reference<WriteBatch> writeBatch;

        public TasksStorageActions(TableStorage tableStorage, IUuidGenerator generator, Reference<SnapshotReader> snapshot, Reference<WriteBatch> writeBatch, IBufferPool bufferPool)
            : base(snapshot, bufferPool)
        {
            this.tableStorage = tableStorage;
            this.generator = generator;
            this.writeBatch = writeBatch;
        }

        public void AddTask(DatabaseTask task, DateTime addedAt)
        {
            var tasksByType = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByType);
            var tasksByIndex = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndex);
            var tasksByIndexAndType = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndexAndType);

            var type = task.GetType().FullName;
            var index = task.Index;
            var id = generator.CreateSequentialUuid(UuidType.Tasks);
            var idAsString = (Slice)id.ToString();

            var taskStructure = new Structure<TaskFields>(tableStorage.Tasks.Schema)
                .Set(TaskFields.IndexId, index)
                .Set(TaskFields.TaskId, id.ToByteArray())
                .Set(TaskFields.AddedAt, addedAt.ToBinary())
                .Set(TaskFields.Type, type)
                .Set(TaskFields.SerializedTask, task.AsBytes());

            tableStorage.Tasks.AddStruct(writeBatch.Value, idAsString, taskStructure, 0);

            var indexKey = CreateKey(index);

            tasksByType.MultiAdd(writeBatch.Value, (Slice)CreateKey(type), idAsString);
            tasksByIndex.MultiAdd(writeBatch.Value, (Slice)indexKey, idAsString);
            tasksByIndexAndType.MultiAdd(writeBatch.Value, (Slice)AppendToKey(indexKey, type), idAsString);
        }

        public bool HasTasks
        {
            get { return ApproximateTaskCount > 0; }
        }

        public long ApproximateTaskCount
        {
            get
            {
                return tableStorage.GetEntriesCount(tableStorage.Tasks);
            }
        }

        public T GetMergedTask<T>(List<int> indexesToSkip, int[] allIndexes, HashSet<IComparable> alreadySeen)
            where T : DatabaseTask
        {
            var type = CreateKey(typeof(T).FullName);
            var tasksByType = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByType);

            using (var iterator = tasksByType.MultiRead(Snapshot, (Slice)type))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    return null;

                do
                {
                    ushort version;
                    var value = LoadStruct(tableStorage.Tasks, iterator.CurrentKey, writeBatch.Value, out version);
                    if (value == null)
                        continue;

                    var currentId = Etag.Parse(value.ReadBytes(TaskFields.TaskId));
                    var indexId = value.ReadInt(TaskFields.IndexId);
                    if (indexesToSkip.Contains(indexId))
                    {
                        if (Logger.IsDebugEnabled)
                            Logger.Debug("Skipping task id: {0} for index id: {1}", currentId, indexId);
                        continue;
                    }

                    if (alreadySeen.Add(currentId) == false)
                        continue;

                    if (allIndexes.Contains(indexId) == false)
                    {
                        if (Logger.IsDebugEnabled)
                            Logger.Debug("Skipping task id: {0} for non existing index id: {0}", currentId, indexId);

                        continue;
                    }

                    DatabaseTask task;
                    try
                    {
                        task = DatabaseTask.ToTask(value.ReadString(TaskFields.Type), value.ReadBytes(TaskFields.SerializedTask));
                    }
                    catch (Exception e)
                    {
                        Logger.ErrorException(
                            string.Format("Could not create instance of a task: {0}", value), e);

                        alreadySeen.Add(currentId);
                        continue;
                    }

                    if (Logger.IsDebugEnabled)
                        Logger.Debug("Fetched task id: {0}", currentId);

                    task.Id = currentId;
                    MergeSimilarTasks(task, alreadySeen, indexesToSkip, allIndexes);

                    return (T)task;
                } while (iterator.MoveNext());
            }

            return null;
        }

        private void MergeSimilarTasks(DatabaseTask task, HashSet<IComparable> alreadySeen, List<int> indexesToSkip, int[] allIndexes)
        {
            string tree;
            Slice slice;
            var type = task.GetType().FullName;
            if (task.SeparateTasksByIndex)
            {
                tree = Tables.Tasks.Indices.ByIndexAndType;
                slice = (Slice)CreateKey(task.Index, type);
            }
            else
            {
                tree = Tables.Tasks.Indices.ByType;
                slice = (Slice)CreateKey(type);
            }

            using (var iterator = tableStorage.Tasks.GetIndex(tree).MultiRead(Snapshot, slice))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    return;

                var totalKeysToProcess = task.NumberOfKeys;
                do
                {
                    if (totalKeysToProcess >= 5 * 1024)
                        break;

                    ushort version;
                    var value = LoadStruct(tableStorage.Tasks, iterator.CurrentKey, writeBatch.Value, out version);
                    if (value == null)
                        continue;

                    var currentId = Etag.Parse(iterator.CurrentKey.ToString());
                    var indexId = value.ReadInt(TaskFields.IndexId);
                    if (indexesToSkip.Contains(indexId))
                    {
                        if (Logger.IsDebugEnabled)
                            Logger.Debug("Skipping task id: {0} for index id: {1}", currentId, indexId);
                        continue;
                    }

                    if (alreadySeen.Add(currentId) == false)
                        continue;

                    if (allIndexes.Contains(indexId) == false)
                    {
                        if (Logger.IsDebugEnabled)
                            Logger.Debug("Skipping task id: {0} for non existing index id: {0}", currentId, indexId);

                        continue;
                    }

                    DatabaseTask existingTask;
                    try
                    {
                        existingTask = DatabaseTask.ToTask(value.ReadString(TaskFields.Type), value.ReadBytes(TaskFields.SerializedTask));
                    }
                    catch (Exception e)
                    {
                        Logger.ErrorException(string.Format("Could not create instance of a task: {0}", value), e);
                        alreadySeen.Add(currentId);
                        continue;
                    }

                    totalKeysToProcess += existingTask.NumberOfKeys;
                    task.Merge(existingTask);
                    if (Logger.IsDebugEnabled)
                        Logger.Debug("Merged task id: {0} with task id: {1}", currentId, task.Id);

                } while (iterator.MoveNext());
            }
        }

        private void RemoveTask(Slice taskId, int index, string type)
        {
            var tasksByType = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByType);
            var tasksByIndex = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndex);
            var tasksByIndexAndType = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndexAndType);

            tableStorage.Tasks.Delete(writeBatch.Value, taskId);

            var indexKey = CreateKey(index);

            tasksByType.MultiDelete(writeBatch.Value, (Slice)CreateKey(type), taskId);
            tasksByIndex.MultiDelete(writeBatch.Value, (Slice)indexKey, taskId);
            tasksByIndexAndType.MultiDelete(writeBatch.Value, (Slice)AppendToKey(indexKey, type), taskId);
        }

        public System.Collections.Generic.IEnumerable<TaskMetadata> GetPendingTasksForDebug()
        {
            if (!HasTasks)
                yield break;

            using (var taskIterator = tableStorage.Tasks.Iterate(Snapshot, writeBatch.Value))
            {
                if (!taskIterator.Seek(Slice.BeforeAllKeys))
                    yield break;

                do
                {
                    ushort version;
                    var taskData = LoadStruct(tableStorage.Tasks, taskIterator.CurrentKey, writeBatch.Value, out version);
                    if (taskData == null)
                        throw new InvalidOperationException("Retrieved a pending task object, but was unable to parse it. This is probably a data corruption or a bug.");

                    TaskMetadata pendingTasksForDebug;
                    try
                    {
                        pendingTasksForDebug = new TaskMetadata
                        {
                            Id = Etag.Parse(taskData.ReadBytes(TaskFields.TaskId)),
                            AddedTime = DateTime.FromBinary(taskData.ReadLong(TaskFields.AddedAt)),
                            Type = taskData.ReadString(TaskFields.Type),
                            IndexId = taskData.ReadInt(TaskFields.IndexId)
                        };
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("The pending task record was parsed, but contained invalid values. See more details at inner exception.", e);
                    }

                    yield return pendingTasksForDebug;
                } while (taskIterator.MoveNext());
            }
        }

        public void DeleteTasks(HashSet<IComparable> alreadySeen)
        {
            foreach (Etag etag in alreadySeen)
            {
                ushort version;
                var taskId = new Slice(etag.ToString());
                var value = LoadStruct(tableStorage.Tasks, taskId, writeBatch.Value, out version);
                if (value == null)
                    continue;

                RemoveTask(taskId, value.ReadInt(TaskFields.IndexId), value.ReadString(TaskFields.Type));
            }
        }

        public int DeleteTasksForIndex(int indexId)
        {
            var count = 0;
            var tasksByIndexAndType = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndex);
            using (var iterator = tasksByIndexAndType.MultiRead(Snapshot, (Slice)CreateKey(indexId)))
            {
                if (iterator.Seek(Slice.BeforeAllKeys) == false)
                    return count;

                do
                {
                    ushort version;
                    var value = LoadStruct(tableStorage.Tasks, iterator.CurrentKey, writeBatch.Value, out version);
                    if (value == null)
                        continue;

                    DatabaseTask task;
                    try
                    {
                        task = DatabaseTask.ToTask(value.ReadString(TaskFields.Type), value.ReadBytes(TaskFields.SerializedTask));
                    }
                    catch (Exception e)
                    {
                        Logger.ErrorException(
                            string.Format("Could not create instance of a task: {0}, for deletion", value),
                            e);
                        continue;
                    }

                    var type = task.GetType().FullName;
                    RemoveTask(iterator.CurrentKey, task.Index, type);
                    count++;
                } while (iterator.MoveNext());
            }

            return count;
        }
    }
}