using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Client.Data;
using Raven.Client.Exceptions;
using Raven.Server.NotificationCenter.Actions;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class DatabaseOperations
    {
        private readonly Logger _logger;
        private readonly DocumentDatabase _db;
        private readonly ConcurrentDictionary<long, Operation> _active = new ConcurrentDictionary<long, Operation>();
        private readonly ConcurrentDictionary<long, Operation> _completed = new ConcurrentDictionary<long, Operation>();

        private long _pendingOperationsCounter;

        public DatabaseOperations(DocumentDatabase db)
        {
            _db = db;
            _logger = LoggingSource.Instance.GetLogger<DatabaseOperations>(db.Name);
        }

        internal void CleanupOperations()
        {
            var twoDaysAgo = SystemTime.UtcNow.AddDays(-2);

            foreach (var taskAndState in _completed)
            {
                var state = taskAndState.Value;

                if (state.Description.EndTime < twoDaysAgo)
                {
                    Operation value;
                    _completed.TryRemove(taskAndState.Key, out value);
                }

                var task = state.Task;
                if (task.Exception != null)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Failed to execute background task {taskAndState.Key}", task.Exception);
                }
            }
        }

        public void KillOperation(long id)
        {
            Operation operation;
            if (_active.TryGetValue(id, out operation) == false)
                return;
            
            if (operation?.Token != null && operation.Task.IsCompleted == false)
            {
                operation.Token.Cancel();
            }
        }

        public Operation GetOperation(long id)
        {
            Operation operation;
            if (_active.TryGetValue(id, out operation))
            {
                return operation;
            }

            if (_completed.TryGetValue(id, out operation))
            {
                return operation;
            }

            return null;
        }

        public Task<IOperationResult> AddOperation(string description, OperationType operationType, Func<Action<IOperationProgress>, Task<IOperationResult>> taskFactory,
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

            var operationDescription = new OperationDescription
            {
                Description = description,
                TaskType = operationType,
                StartTime = SystemTime.UtcNow
            };

            var operation = new Operation
            {
                Id = id,
                Description = operationDescription,
                Token = token,
                State = operationState
            };

            Action<IOperationProgress> action = progress =>
            {
                notification.State.Progress = progress;
                RaiseNotifications(notification, operation);
            };

            operation.Task = taskFactory(action);
            
            operation.Task.ContinueWith(taskResult =>
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

                    var isConflict = innerException is DocumentConflictException || innerException is ConcurrencyException;
                    var status = isConflict ? HttpStatusCode.Conflict : HttpStatusCode.InternalServerError;

                    var shouldPersist = false;

                    switch (operationType)
                    {
                        case OperationType.DatabaseExport:
                        case OperationType.DatabaseImport:
                            shouldPersist = true;
                            break;
                    }

                    operationState.Result = new OperationExceptionResult(innerException, status, shouldPersist);
                    operationState.Status = OperationStatus.Faulted;
                }
                else
                {
                    operationState.Result = taskResult.Result;
                    operationState.Status = OperationStatus.Completed;
                }

                Operation completed;
                if (_active.TryGetValue(id, out completed))
                {
                    // add to completed items before removing from active ones to ensure an operation status is accessible all the time
                    _completed.TryAdd(id, completed);
                    _active.TryRemove(id, out completed);
                }
                
                RaiseNotifications(notification, operation);
            });

            _active.TryAdd(id, operation);

            return operation.Task;
        }

        private void RaiseNotifications(OperationStatusChangeNotification notification, Operation operation)
        {
            var operationChanged = OperationChanged.Create(notification.OperationId, operation.Description, notification.State, operation.Killable);

            operation.NotifyCenter(operationChanged, x => _db.NotificationCenter.Add(x));

            _db.Notifications.RaiseNotifications(notification);
        }

        public void KillRunningOperation(long id)
        {
            Operation value;
            if (_active.TryGetValue(id, out value))
            {
                if (value.Task.IsCompleted == false)
                {
                    value.Token?.Cancel();

                    // add to completed items before removing from active ones to ensure an operation status is accessible all the time
                    _completed.TryAdd(id, value);
                    _active.TryRemove(id, out value);
                }
            }
        }

        public long GetNextOperationId()
        {
            return Interlocked.Increment(ref _pendingOperationsCounter);
        }

        public void Dispose(ExceptionAggregator exceptionAggregator)
        {
            foreach (var pendingTaskAndState in _active.Values)
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

            _active.Clear();
            _completed.Clear();
        }

        public IEnumerable<Operation> GetAll()
        {
            return _active.Values.Union(_completed.Values);
        }

        public ICollection<Operation> GetActive()
        {
            return _active.Values;
        }

        public class Operation
        {
            private readonly TimeSpan _throttleTime = TimeSpan.FromSeconds(1);

            private readonly ThrottledNotification _throttle = new ThrottledNotification();

            public long Id;

            [JsonIgnore]
            public Task<IOperationResult> Task;

            [JsonIgnore]
            public OperationCancelToken Token;

            public OperationDescription Description;

            public OperationState State;

            public bool Killable => Token != null;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Id)] = Id,
                    [nameof(Description)] = Description.ToJson(),
                    [nameof(Killable)] = Killable,
                    [nameof(State)] = State.ToJson()
                };
            }

            public void NotifyCenter(OperationChanged notification, Action<OperationChanged> addToNotificationCenter)
            {
                if (notification.State.Status != OperationStatus.InProgress)
                {
                    addToNotificationCenter(notification);
                    return;
                }

                // let us throttle notifications about the operation progress

                var now = SystemTime.UtcNow;

                _throttle.Notification = notification;

                var sinceLastSent = now - _throttle.SentAt;

                if (_throttle.Scheduled == null && sinceLastSent > _throttleTime)
                {
                    addToNotificationCenter(_throttle.Notification);
                    _throttle.SentAt = now;

                    return;
                }

                if (_throttle.Scheduled == null)
                {
                    _throttle.Scheduled = System.Threading.Tasks.Task.Delay(_throttleTime - sinceLastSent).ContinueWith(x =>
                    {
                        if (State.Status == OperationStatus.InProgress)
                            addToNotificationCenter(_throttle.Notification);

                        _throttle.SentAt = DateTime.UtcNow;
                        _throttle.Scheduled = null;
                    });
                }
            }

            private class ThrottledNotification
            {
                public OperationChanged Notification;

                public DateTime SentAt;

                public Task Scheduled;
            }
        }

        public class OperationDescription
        {
            public string Description;
            public OperationType TaskType;
            public DateTime StartTime;
            public DateTime EndTime;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    ["Description"] = Description,
                    ["TaskType"] = TaskType.ToString(),
                    ["StartTime"] = StartTime,
                    ["EndTime"] = EndTime
                };
            }
        }

        public enum OperationType
        {
            [Description("Update by index")]
            UpdateByIndex,

            [Description("Delete by index")]
            DeleteByIndex,

            [Description("Database export")]
            DatabaseExport,

            [Description("Database import")]
            DatabaseImport,

            [Description("Index compact")]
            IndexCompact,

            [Description("Delete by collection")]
            DeleteByCollection,

            [Description("Update by collection")]
            UpdateByCollection

        }
    }
}