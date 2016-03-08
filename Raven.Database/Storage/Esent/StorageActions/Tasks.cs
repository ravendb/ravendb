//-----------------------------------------------------------------------
// <copyright file="Tasks.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Logging;
using Raven.Database.Tasks;

namespace Raven.Database.Storage.Esent.StorageActions
{
    public partial class DocumentStorageActions : ITasksStorageActions
    {
        private static int bookmarkMost = SystemParameters.BookmarkMost;

        public void AddTask(DatabaseTask task, DateTime addedAt)
        {
            int actualBookmarkSize;
            var bookmark = new byte[bookmarkMost];
            using (var update = new Update(session, Tasks, JET_prep.Insert))
            {
                Api.SetColumn(session, Tasks, tableColumnsCache.TasksColumns["task"], task.AsBytes());
                Api.SetColumn(session, Tasks, tableColumnsCache.TasksColumns["for_index"], task.Index);
                Api.SetColumn(session, Tasks, tableColumnsCache.TasksColumns["task_type"], task.GetType().FullName, Encoding.ASCII);
                Api.SetColumn(session, Tasks, tableColumnsCache.TasksColumns["added_at"], addedAt.ToBinary());

                update.Save(bookmark, bookmark.Length, out actualBookmarkSize);
            }
            Api.JetGotoBookmark(session, Tasks, bookmark, actualBookmarkSize);
        }

        public bool HasTasks
        {
            get
            {
                return Api.TryMoveFirst(session, Tasks);
            }
        }

        public long ApproximateTaskCount
        {
            get
            {
                if (Api.TryMoveFirst(session, Tasks) == false)
                    return 0;
                var first = (int)Api.RetrieveColumnAsInt32(session, Tasks, tableColumnsCache.TasksColumns["id"]);
                if (Api.TryMoveLast(session, Tasks) == false)
                    return 0;
                var last = (int)Api.RetrieveColumnAsInt32(session, Tasks, tableColumnsCache.TasksColumns["id"]);

                var result = last - first;
                return result + 1;
            }
        }

        public T GetMergedTask<T>(List<int> indexesToSkip, int[] allIndexes, HashSet<IComparable> alreadySeen)
            where T : DatabaseTask
        {
            var expectedTaskType = typeof(T).FullName;
            Api.JetSetCurrentIndex(session, Tasks, "by_task_type");

            Api.MakeKey(session, Tasks, expectedTaskType, Encoding.ASCII, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Tasks, SeekGrbit.SeekEQ) == false)
            {
                return null;
            }

            Api.MakeKey(session, Tasks, expectedTaskType, Encoding.ASCII, MakeKeyGrbit.NewKey);
            Api.JetSetIndexRange(session, Tasks, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

            do
            {
                var taskType = Api.RetrieveColumnAsString(session, Tasks, tableColumnsCache.TasksColumns["task_type"], Encoding.ASCII);
                // esent index ranges are approximate, and we need to check them ourselves as well
                if (taskType != expectedTaskType)
                {
                    //this shouldn't happen
                    logger.Warn("Tasks type mismatch: expected task type: {0}, current task type: {1}",
                        expectedTaskType, taskType);
                    continue;
                }

                var currentId = Api.RetrieveColumnAsInt32(session, Tasks, tableColumnsCache.TasksColumns["id"]).Value;
                var index = Api.RetrieveColumnAsInt32(session, Tasks, tableColumnsCache.TasksColumns["for_index"]).Value;
                if (indexesToSkip.Contains(index))
                {
                    if (logger.IsDebugEnabled)
                        logger.Debug("Skipping task id: {0} for index id: {1}", currentId, index);
                    continue;
                }

                if (alreadySeen.Add(currentId) == false)
                    continue;

                if (allIndexes.Contains(index) == false)
                {
                    if (logger.IsDebugEnabled)
                        logger.Debug("Skipping task id: {0} for non existing index id: {0}", currentId, index);

                    continue;
                }

                var taskAsBytes = Api.RetrieveColumn(session, Tasks, tableColumnsCache.TasksColumns["task"]);
                DatabaseTask task;
                try
                {
                    task = DatabaseTask.ToTask(taskType, taskAsBytes);
                }
                catch (Exception e)
                {
                    logger.ErrorException(
                        string.Format("Could not create instance of a task: {0}", taskAsBytes),
                        e);

                    alreadySeen.Add(currentId);
                    continue;
                }

                if (logger.IsDebugEnabled)
                    logger.Debug("Fetched task id: {0}", currentId);

                task.Id = currentId;
                MergeSimilarTasks(task, alreadySeen, indexesToSkip, allIndexes);

                return (T)task;

            } while (Api.TryMoveNext(session, Tasks));

            return null;
        }

        public IEnumerable<TaskMetadata> GetPendingTasksForDebug()
        {
            Api.MoveBeforeFirst(session, Tasks);
            while (Api.TryMoveNext(session, Tasks))
            {
                var type = Api.RetrieveColumnAsString(session, Tasks, tableColumnsCache.TasksColumns["task_type"], Encoding.ASCII);
                var index = Api.RetrieveColumnAsInt32(session, Tasks, tableColumnsCache.TasksColumns["for_index"]);
                var addedTime64 = Api.RetrieveColumnAsInt64(session, Tasks, tableColumnsCache.TasksColumns["added_at"]).Value;
                var id = Api.RetrieveColumnAsInt32(session, Tasks, tableColumnsCache.TasksColumns["id"]).Value;

                yield return new TaskMetadata
                {
                    Id = id,
                    AddedTime = DateTime.FromBinary(addedTime64),
                    IndexId = index ?? -1,
                    Type = type
                };
            }
        }

        private void MergeSimilarTasks(DatabaseTask task, HashSet<IComparable> alreadySeen, List<int> indexesToSkip, int[] allIndexes)
        {
            var expectedTaskType = task.GetType().FullName;

            if (task.SeparateTasksByIndex)
            {
                Api.JetSetCurrentIndex(session, Tasks, "by_index_and_task_type");

                Api.MakeKey(session, Tasks, task.Index, MakeKeyGrbit.NewKey);
                Api.MakeKey(session, Tasks, expectedTaskType, Encoding.ASCII, MakeKeyGrbit.None);
                if (Api.TrySeek(session, Tasks, SeekGrbit.SeekEQ) == false)
                {
                    // there are no tasks matching the current one, just return
                    return;
                }

                Api.MakeKey(session, Tasks, task.Index, MakeKeyGrbit.NewKey);
                Api.MakeKey(session, Tasks, expectedTaskType, Encoding.ASCII, MakeKeyGrbit.None);
                Api.JetSetIndexRange(session, Tasks, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
            }
            else
            {
                Api.JetSetCurrentIndex(session, Tasks, "by_task_type");

                Api.MakeKey(session, Tasks, expectedTaskType, Encoding.ASCII, MakeKeyGrbit.NewKey);
                if (Api.TrySeek(session, Tasks, SeekGrbit.SeekEQ) == false)
                {
                    // there are no tasks matching the current one, just return
                    return;
                }

                Api.MakeKey(session, Tasks, expectedTaskType, Encoding.ASCII, MakeKeyGrbit.NewKey);
                Api.JetSetIndexRange(session, Tasks, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
            }

            var totalKeysToProcess = task.NumberOfKeys;
            do
            {
                if (totalKeysToProcess >= 5 * 1024)
                    break;

                var taskType = Api.RetrieveColumnAsString(session, Tasks, tableColumnsCache.TasksColumns["task_type"], Encoding.ASCII);
                // esent index ranges are approximate, and we need to check them ourselves as well
                if (taskType != expectedTaskType)
                {
                    //this shouldn't happen
                    logger.Warn("Tasks type mismatch: expected task type: {0}, current task type: {1}", 
                        expectedTaskType, taskType);
                    continue;
                }
                    

                var currentId = Api.RetrieveColumnAsInt32(session, Tasks, tableColumnsCache.TasksColumns["id"]).Value;
                var index = Api.RetrieveColumnAsInt32(session, Tasks, tableColumnsCache.TasksColumns["for_index"]).Value;
                if (task.SeparateTasksByIndex == false && indexesToSkip.Contains(index))
                {
                    //need to check this only when not separating tasks by index
                    if (logger.IsDebugEnabled)
                        logger.Debug("Skipping task id: {0} for index id: {1}", currentId, index);
                    continue;
                }

                if (alreadySeen.Add(currentId) == false)
                    continue;

                if (task.SeparateTasksByIndex == false && allIndexes.Contains(index) == false)
                {
                    //need to check this only when not separating tasks by index
                    if (logger.IsDebugEnabled)
                        logger.Debug("Skipping task id: {0} for non existing index id: {0}", currentId, index);

                    continue;
                }

                var taskAsBytes = Api.RetrieveColumn(session, Tasks, tableColumnsCache.TasksColumns["task"]);
                DatabaseTask existingTask;
                try
                {
                    existingTask = DatabaseTask.ToTask(taskType, taskAsBytes);
                }
                catch (Exception e)
                {
                    logger.ErrorException(
                        string.Format("Could not create instance of a task: {0}", taskAsBytes),
                        e);

                    alreadySeen.Add(currentId);
                    continue;
                }

                totalKeysToProcess += existingTask.NumberOfKeys;
                task.Merge(existingTask);
                if (logger.IsDebugEnabled)
                    logger.Debug("Merged task id: {0} with task id: {1}", currentId, task.Id);

            } while (Api.TryMoveNext(session, Tasks));
        }

        public void DeleteTasks(HashSet<IComparable> alreadySeen)
        {
            Api.JetSetCurrentIndex(session, Tasks, "by_id");
            foreach (var taskId in alreadySeen)
            {
                Api.MakeKey(session, Tasks, (int)taskId, MakeKeyGrbit.NewKey);
                if (Api.TrySeek(session, Tasks, SeekGrbit.SeekEQ) == false)
                {
                    // there is no matching task
                    continue;
                }
                Api.JetDelete(session, Tasks);
            }
        }

        public int DeleteTasksForIndex(int indexId)
        {
            var count = 0;

            Api.JetSetCurrentIndex(session, Tasks, "by_index");
            Api.MakeKey(session, Tasks, indexId, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Tasks, SeekGrbit.SeekEQ) == false)
            {
                // there are no tasks matching the current one, just return
                return count;
            }

            Api.MakeKey(session, Tasks, indexId, MakeKeyGrbit.NewKey);
            Api.JetSetIndexRange(session, Tasks, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

            do
            {
                try
                {
                    Api.JetDelete(session, Tasks);
                    count++;
                }
                catch (EsentErrorException e)
                {
                    if (e.Error != JET_err.WriteConflict)
                        throw;
                }
            } while (Api.TryMoveNext(session, Tasks));

            return count;
        }
    }
}
