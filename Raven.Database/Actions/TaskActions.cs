// -----------------------------------------------------------------------
//  <copyright file="TaskActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Data;
using Raven.Database.Impl;
using Raven.Database.Util;

namespace Raven.Database.Actions
{
    public class TaskActions : ActionsBase
    {
        private long pendingTaskCounter;
        private readonly ConcurrentDictionary<long, PendingTaskAndState> pendingTasks = new ConcurrentDictionary<long, PendingTaskAndState>();

        public TaskActions(DocumentDatabase database, SizeLimitedConcurrentDictionary<string, TouchedDocumentInfo> recentTouches, IUuidGenerator uuidGenerator, ILog log)
            : base(database, recentTouches, uuidGenerator, log)
        {
        }

        internal void ClearCompletedPendingTasks()
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
                    Log.InfoException("Failed to execute background task " + taskAndState.Key, task.Exception);
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

        public void RemoveTask(long taskId)
        {
            PendingTaskAndState value;
            pendingTasks.TryRemove(taskId, out value);
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

        public void Dispose(ExceptionAggregator exceptionAggregator)
        {
            foreach (var pendingTaskAndState in pendingTasks.Select(shouldDispose => shouldDispose.Value))
            {
                exceptionAggregator.Execute(() =>
                {
                    try
                    {
#if DEBUG
                        pendingTaskAndState.Task.Wait(3000);
#else
						pendingTaskAndState.Task.Wait();
#endif
                    }
                    catch (Exception)
                    {
                        // we explictly don't care about this during shutdown
                    }
                });
            }

            pendingTasks.Clear();
        }

        private class PendingTaskAndState
        {
            public Task Task;
            public object State;
        }
    }
}