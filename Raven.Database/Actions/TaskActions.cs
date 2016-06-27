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
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
    public class TaskActions : ActionsBase
    {
        private long pendingTaskCounter;
        private readonly ConcurrentDictionary<long, PendingTask> pendingTasks = new ConcurrentDictionary<long, PendingTask>();

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
                    PendingTask value;
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
                var task = new PendingTaskDescriptionAndStatus
                {
                    Id = x.Key,
                    TaskType = x.Value.Description.TaskType,
                    Description = x.Value.Description.Description,
                    StartTime = x.Value.Description.StartTime,
                    Status = x.Value.State.State,
                    Completed = x.Value.State.Completed,
                    Faulted = x.Value.State.Faulted,
                    Canceled = x.Value.State.Canceled,
                    Exception = x.Value.State.Exception,
                    Killable = x.Value.TokenSource != null && !x.Value.Task.IsCompleted
                };

                // create thread safe copy of status to avoid InvalidOperationException during serialization 
                if (task.Status != null)
                {
                    lock (task.Status)
                    {
                        task.Status = task.Status.CloneToken();
                    }
                }
              
                return task;
            }).ToList();
        }

        public void AddTask(Task task, IOperationState state, PendingTaskDescription description, out long id, CancellationTokenSource tokenSource = null)
        {
            if (task.Status == TaskStatus.Created)
                throw new ArgumentException("Task must be started before it gets added to the database.", "task");
            var localId = id = Interlocked.Increment(ref pendingTaskCounter);
            pendingTasks.TryAdd(localId, new PendingTask
            {
                Task = task,
                State = state,
                Description = description,
                TokenSource = tokenSource
            });
        }

        public void RemoveTask(long taskId)
        {
            PendingTask value;
            pendingTasks.TryRemove(taskId, out value);
        }


        public IOperationState KillTask(long id)
        {
            PendingTask value;
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

        public IOperationState GetTaskState(long id)
        {
            PendingTask value;
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

        public class PendingTask
        {
            public Task Task;
            public IOperationState State;
            public PendingTaskDescription Description;
            public CancellationTokenSource TokenSource;
        }

        /// <summary>
        /// Used for describing task before it is even started
        /// </summary>
        public class PendingTaskDescription
        {
            public string Description;
            public PendingTaskType TaskType;
            public DateTime StartTime;
        }

        public class PendingTaskDescriptionAndStatus : PendingTaskDescription
        {
            public long Id;
            public RavenJToken Status { get; set; }
            public Exception Exception;
            public bool Killable;
            public bool Completed { get; set; }
            public bool Faulted { get; set; }
            public bool Canceled { get; set; }
        }

        public enum PendingTaskType
        {
            SuggestionQuery,

            BulkInsert,

            IndexBulkOperation,

            IndexDeleteOperation,

            BackupDatabase,

            BackupFilesystem,

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

            ResolveConflicts,

            StorageBreakdown,

            SlowDocCounts
        }
    }
}

