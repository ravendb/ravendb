using System;
using System.IO;
using log4net;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Storage.StorageActions;
using Raven.Database.Tasks;
using Raven.Storage.Managed.Impl;
using System.Linq;
using Raven.Database.Extensions;

namespace Raven.Storage.Managed
{
    public class TasksStorageActions : ITasksStorageActions
    {
        private readonly TableStorage storage;
        private ILog logger = LogManager.GetLogger(typeof(TasksStorageActions));

        public TasksStorageActions(TableStorage storage)
        {
            this.storage = storage;
        }

        public void AddTask(Task task, DateTime addedAt)
        {
            storage.Tasks.Put(new JObject
            {
                {"index", task.Index},
                {"id", DocumentDatabase.CreateSequentialUuid().ToByteArray()},
                {"time", addedAt},
                {"type", task.Type},
                {"mergable", task.SupportsMerging}
            }, task.AsBytes());
        }

        public bool HasTasks
        {
            get { return ApproximateTaskCount > 0; }
        }

        public long ApproximateTaskCount
        {
            get { return storage.Tasks.Count; }
        }

        public Task GetMergedTask(out int countOfMergedTasks)
        {
            foreach (var readResult in storage.Tasks)
            {
                Task task;
                try
                {
                    task = Task.ToTask(readResult.Key.Value<string>("type"), readResult.Data());
                }
                catch (Exception e)
                {
                    logger.ErrorFormat(e, "Could not create instance of a task: {0}", readResult.Key);
                    continue;
                }
                MergeSimilarTasks(task, readResult.Key.Value<byte[]>("id"), out countOfMergedTasks);
                storage.Tasks.Remove(readResult.Key);
                return task;
            }
            countOfMergedTasks = 0;
            return null;
        }

        private void MergeSimilarTasks(Task task, byte [] taskId, out int taskCount)
        {
            taskCount = 1;
            if (task.SupportsMerging == false)
                return;

            var keyForTaskToTryMergings = storage.Tasks["ByIndexAndType"].SkipTo(new JObject
            {
                {"index", task.Index},
                {"type", task.Type},
            })
            .Where(x => new Guid(x.Value<byte[]>("id")) != new Guid(taskId))
                .TakeWhile(x =>
                           StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("index"), task.Index) &&
                           StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("type"), task.Type)
                );

            foreach (var keyForTaskToTryMerging in keyForTaskToTryMergings)
            {
                var readResult = storage.Tasks.Read(keyForTaskToTryMerging);
                if(readResult == null)
                    continue;
                Task existingTask;
                try
                {
                    existingTask = Task.ToTask(readResult.Key.Value<string>("type"), readResult.Data());
                }
                catch (Exception e)
                {
                    logger.ErrorFormat(e, "Could not create instance of a task: {0}", readResult.Key);
                    storage.Tasks.Remove(keyForTaskToTryMerging);
                    continue;
                }

                if (task.TryMerge(existingTask) == false)
                    continue;

                storage.Tasks.Remove(keyForTaskToTryMerging);

                taskCount += 1;

                if (task.SupportsMerging == false)
                    return;
            }
        }

    }
}