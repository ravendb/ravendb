// -----------------------------------------------------------------------
//  <copyright file="TaskActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Database.Actions
{
    public class TaskActions
    {
        private void ClearCompletedPendingTasks()
        {
            foreach (var taskAndState in pendingTasks)
            {
                var task = taskAndState.Value.Task;
                if (task.IsCompleted || task.IsCanceled || task.IsFaulted)
                {
                    PendingTaskAndState value;
                    pendingTasks.TryRemove(taskAndState.Key, out value);
                }
                if (task.Exception != null)
                {
                    log.InfoException("Failed to execute background task " + taskAndState.Key, task.Exception);
                }
            }
        }

        public void AddTask(Task task, object state, out long id)
        {
            if (task.Status == TaskStatus.Created)
                throw new ArgumentException("Task must be started before it gets added to the database.", "task");
            var localId = id = Interlocked.Increment(ref pendingTaskCounter);
            pendingTasks.TryAdd(localId, new PendingTaskAndState
            {
                Task = task,
                State = state
            });
        }

        public object GetTaskState(long id)
        {
            PendingTaskAndState value;
            if (pendingTasks.TryGetValue(id, out value))
            {
                if (value.Task.IsFaulted || value.Task.IsCanceled)
                {
                    var ex = value.Task.Exception.ExtractSingleInnerException();
                    throw ex;
                }

                return value.State;
            }
            return null;
        }
    }
}