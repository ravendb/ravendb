// -----------------------------------------------------------------------
//  <copyright file="TaskActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
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
        private readonly ConcurrentDictionary<long, PendingTaskWithStateAndDescription> pendingTasks = new ConcurrentDictionary<long, PendingTaskWithStateAndDescription>();

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
                    PendingTaskWithStateAndDescription value;
                    pendingTasks.TryRemove(taskAndState.Key, out value);
                }
                if (task.Exception != null)
                {
                    Log.InfoException("Failed to execute background task " + taskAndState.Key, task.Exception);
                }
            }
        }

        public List<PendingTaskDescriptionAndStatus> GetAll()
        {
            return pendingTasks.Select(x =>
            {
                var ex = (x.Value.Task.IsFaulted || x.Value.Task.IsCanceled) ? x.Value.Task.Exception.ExtractSingleInnerException() : null;
                var taskStatus = x.Value.Task.Status;
                if (taskStatus == TaskStatus.WaitingForActivation)
                    taskStatus = TaskStatus.Running; //aysnc task status is always WaitingForActivation

                if (ex == null && x.Value.Task.IsCompleted && x.Value.State != null)
                {
                    // alternatively try to override task status based on stored status
                    if (x.Value.State.Faulted)
                    {
                        taskStatus = TaskStatus.Faulted;
                        ex = new Exception(x.Value.State.State.Value<string>("Error"));
                    }
                }

                return new PendingTaskDescriptionAndStatus
                       {
                           Id = x.Key,
                           Payload = x.Value.Description.Payload,
                           StartTime = x.Value.Description.StartTime,
                           TaskStatus = taskStatus,
                           TaskType = x.Value.Description.TaskType,
                           Exception = ex
                       };
            }).ToList();
        }

        public void AddTask(Task task, IOperationState state, PendingTaskDescription description, out long id, CancellationTokenSource tokenSource = null)
        {
            if (task.Status == TaskStatus.Created)
                throw new ArgumentException("Task must be started before it gets added to the database.", "task");
            var localId = id = Interlocked.Increment(ref pendingTaskCounter);
            pendingTasks.TryAdd(localId, new PendingTaskWithStateAndDescription
            {
                Task = task,
                State = state,
                Description = description,
                TokenSource = tokenSource
            });
        }

        public void RemoveTask(long taskId)
        {
            PendingTaskWithStateAndDescription value;
            pendingTasks.TryRemove(taskId, out value);
        }


        public object KillTask(long id)
        {
            PendingTaskWithStateAndDescription value;
            if (pendingTasks.TryGetValue(id, out value))
            {
                if (!value.Task.IsFaulted && !value.Task.IsCanceled && !value.Task.IsCompleted)
                {
                    if (value.TokenSource != null)
                    {
                        value.TokenSource.Cancel();
                    }
                }

                return value.State;
            }
            return null;
        }

        public object GetTaskState(long id)
        {
            PendingTaskWithStateAndDescription value;
            if (pendingTasks.TryGetValue(id, out value))
            {
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

        public class PendingTaskWithStateAndDescription
        {
            public Task Task;
            public IOperationState State;
            public PendingTaskDescription Description;
            public CancellationTokenSource TokenSource;
        }

        public class PendingTaskDescription
        {
            public string Payload;

            public PendingTaskType TaskType;

            public DateTime StartTime;
        }

        public class PendingTaskDescriptionAndStatus : PendingTaskDescription
        {
            public long Id;
            public TaskStatus TaskStatus;
            public Exception Exception;
        }

        public enum PendingTaskType
        {
            SuggestionQuery,

            BulkInsert,

            IndexBulkOperation,

            IndexDeleteOperation,

            ImportDatabase,

            RestoreDatabase,

            RestoreFilesystem,

            CompactDatabase,

            CompactFilesystem,

            IoTest,

            NewIndexPrecomputedBatch,

            PurgeTombstones,

            ServerSmuggling,

            CounterBatchOperation,

            TimeSeriesBatchOperation,

            RecoverCorruptedIndexOperation,

            ResolveConflicts
        }
    }
}
