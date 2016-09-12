using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Client.Data;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class DatabaseOperations
    {
        private readonly Logger _logger;
        private readonly DocumentDatabase _db;
        private readonly ConcurrentDictionary<long, PendingOperation> _pendingOperations = new ConcurrentDictionary<long, PendingOperation>();

        private long _pendingOperationsCounter;

        public DatabaseOperations(DocumentDatabase db)
        {
            _db = db;
            _logger = LoggingSource.Instance.GetLogger<DatabaseOperations>(db.Name);
        }

        internal void ClearCompletedPendingTasks()
        {
            foreach (var taskAndState in _pendingOperations)
            {
                var task = taskAndState.Value.Task;
                if (task.IsCompleted || task.IsCanceled || task.IsFaulted)
                {
                    PendingOperation value;
                    _pendingOperations.TryRemove(taskAndState.Key, out value);
                }
                if (task.Exception != null)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Failed to execute background task {taskAndState.Key}", task.Exception);
                }
            }
        }

        //TODO: get all

        public OperationState GetOperationState(long id)
        {
            PendingOperation operation;
            if (_pendingOperations.TryGetValue(id, out operation))
            {
                return operation.State;
            }
            return null;
        }

        public Task<IOperationResult> AddOperation(string description, PendingOperationType opererationType, Func<Action<IOperationProgress>, Task<IOperationResult>> taskFactory, 
            long id, OperationCancelToken token = null)
        {
            var operationState = new OperationState
            {
                Status = OperationStatus.InProgress
            };

            var notification = new OperationStatusChangeNotification
            {
                OperationId = id,
                State = operationState
            };

            Action<IOperationProgress> action = progress =>
            {
                notification.State.Progress = progress;
                RaiseNotifications(notification);
            };
            var task = taskFactory(action);

            var operationDescription = new PendingOperationDescription
            {
                Description = description,
                TaskType = opererationType,
                StartTime = SystemTime.UtcNow
            };

            var pendingOperation = new PendingOperation
            {
                Task = task,
                Description = operationDescription,
                Token = token,
                State = operationState
            };

            task.ContinueWith(taskResult =>
            {
                operationDescription.EndTime = SystemTime.UtcNow;
                operationState.Progress = null;
                if (taskResult.IsCanceled)
                {
                    operationState.Result = null;
                    operationState.Status = OperationStatus.Canceled;
                }
                else if (taskResult.IsFaulted)
                {
                    var innerException = taskResult.Exception.ExtractSingleInnerException();
                    operationState.Result = new OperationExceptionResult(innerException);
                    operationState.Status = OperationStatus.Faulted;
                }
                else
                {
                    operationState.Result = taskResult.Result;
                    operationState.Status = OperationStatus.Completed;
                }

                RaiseNotifications(notification);
            });

            _pendingOperations.TryAdd(id, pendingOperation);

            return task;
        }

        private void RaiseNotifications(OperationStatusChangeNotification notification)
        {
            _db.Notifications.RaiseNotifications(notification);
        }

        public void KillRunningOperation(long id)
        {
            PendingOperation value;
            if (_pendingOperations.TryGetValue(id, out value))
            {
                if (value.Task.IsCompleted == false)
                {
                    value.Token?.Cancel();
                }
            }
        }

        public long GetNextOperationId()
        {
            return Interlocked.Increment(ref _pendingOperationsCounter);
        }

        public void RemoveOperation(long operationId)
        {
            PendingOperation value;
            _pendingOperations.TryRemove(operationId, out value);
        }

        public void Dispose(ExceptionAggregator exceptionAggregator)
        {
            foreach (var pendingTaskAndState in _pendingOperations.Values)
            {
                exceptionAggregator.Execute(() =>
                {
                    try
                    {
                        pendingTaskAndState.Task.Wait();
                    }
                    catch (Exception)
                    {
                        // we explictly don't care about this during shutdown
                    }
                });
            }

            _pendingOperations.Clear();
        }

        public class PendingOperation
        {
            public Task Task;
            public PendingOperationDescription Description;
            public OperationCancelToken Token;
            public OperationState State;
        }

        public class PendingOperationDescription
        {
            public string Description;
            public PendingOperationType TaskType;
            public DateTime StartTime;
            public DateTime EndTime;
        }

        public enum PendingOperationType 
        {
            UpdateByIndex,

            DeleteByIndex

            //TODO: other operation types
        }

       
    }
}