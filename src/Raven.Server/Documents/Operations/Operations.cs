using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.LowMemory;

namespace Raven.Server.Documents.Operations
{
    public class Operations : ILowMemoryHandler
    {
        private readonly Logger _logger;
        private readonly ConcurrentDictionary<long, Operation> _active = new ConcurrentDictionary<long, Operation>();
        private readonly ConcurrentDictionary<long, Operation> _completed = new ConcurrentDictionary<long, Operation>();
        private readonly string _name;
        private readonly OperationsStorage _operationsStorage;
        private readonly NotificationCenter.NotificationCenter _notificationCenter;
        private readonly DocumentsChanges _changes;
        private readonly TimeSpan _maxCompletedTaskLifeTime;

        public Operations(string name,
            OperationsStorage operationsStorage,
            NotificationCenter.NotificationCenter notificationCenter,
            DocumentsChanges changes,
            TimeSpan maxCompletedTaskLifeTime)
        {
            _name = name;
            _operationsStorage = operationsStorage;
            _notificationCenter = notificationCenter;
            _changes = changes;
            _maxCompletedTaskLifeTime = maxCompletedTaskLifeTime;
            _logger = LoggingSource.Instance.GetLogger<Operations>(name ?? "Server");
            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
        }

        internal void CleanupOperations()
        {
            CleanupOperationsInternal(_completed, _maxCompletedTaskLifeTime);
        }

        private static void CleanupOperationsInternal(ConcurrentDictionary<long, Operation> operations, TimeSpan maxCompletedTaskLifeTime)
        {
            var oldestPossibleCompletedOperation = SystemTime.UtcNow - maxCompletedTaskLifeTime;

            foreach (var taskAndState in operations)
            {
                var state = taskAndState.Value;

                if (state.Description.EndTime.HasValue && state.Description.EndTime < oldestPossibleCompletedOperation)
                {
                    operations.TryRemove(taskAndState.Key, out Operation _);
                }
            }
        }

        public void KillOperation(long id)
        {
            if (_active.TryGetValue(id, out Operation operation) == false)
                throw new ArgumentException($"Operation {id} was not registered");

            if (operation?.Token != null && operation.IsCompleted() == false)
            {
                operation.Token.Cancel();
            }

            if (operation?.Killable == false)
                throw new ArgumentException($"Operation {id} is unkillable");
        }

        public Operation GetOperation(long id)
        {
            if (_active.TryGetValue(id, out Operation operation))
            {
                return operation;
            }

            if (_completed.TryGetValue(id, out operation))
            {
                return operation;
            }

            return null;
        }

        public Task<IOperationResult> AddOperation(
            DocumentDatabase database,
            string description,
            OperationType operationType,
            Func<Action<IOperationProgress>, Task<IOperationResult>> taskFactory,
            long id,
            IOperationDetailedDescription detailedDescription = null,
            OperationCancelToken token = null)
        {
            var operationState = new OperationState
            {
                Status = OperationStatus.InProgress
            };

            var notification = new OperationStatusChange
            {
                OperationId = id,
                State = operationState
            };

            var operationDescription = new OperationDescription
            {
                Description = description,
                TaskType = operationType,
                StartTime = SystemTime.UtcNow,
                DetailedDescription = detailedDescription
            };

            var operation = new Operation
            {
                Database = database,
                Id = id,
                Description = operationDescription,
                Token = token,
                State = operationState
            };

            void ProgressNotification(IOperationProgress progress)
            {
                notification.State.Progress = progress;
                RaiseNotifications(notification, operation);
            }

            operation.Task = Task.Run(() => taskFactory(ProgressNotification));

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

                if (_active.TryGetValue(id, out Operation completed))
                {
                    completed.SetCompleted();
                    // add to completed items before removing from active ones to ensure an operation status is accessible all the time
                    _completed.TryAdd(id, completed);
                    _active.TryRemove(id, out completed);
                }

                RaiseNotifications(notification, operation);
            });

            _active.TryAdd(id, operation);

            if (token == null)
                return operation.Task;

            return operation.Task.ContinueWith(t =>
            {
                token.Dispose();
                return t;
            }).Unwrap();
        }

        private void RaiseNotifications(OperationStatusChange change, Operation operation)
        {
            var operationChanged = OperationChanged.Create(_name, change.OperationId, operation.Description, change.State, operation.Killable);

            operation.NotifyCenter(operationChanged, x => _notificationCenter.Add(x));

            _changes?.RaiseNotifications(change);
        }

        public long GetNextOperationId()
        {
            return _operationsStorage.GetNextOperationId();
        }

        public void Dispose(ExceptionAggregator exceptionAggregator)
        {
            foreach (var active in _active.Values)
            {
                exceptionAggregator.Execute(() =>
                {
                    try
                    {
                        if (active.Killable)
                            active.Token.Cancel();

                        active.Task?.Wait();
                    }
                    catch (Exception)
                    {
                        // we explicitly don't care about this during shutdown
                    }
                });
            }

            _active.Clear();
            _completed.Clear();
        }

        public IEnumerable<Operation> GetAll() => _active.Values.Union(_completed.Values);

        public ICollection<Operation> GetActive() => _active.Values;

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            // cleanup operations older than 1 minute only
            // Client API might still be waiting for the status
            CleanupOperationsInternal(_completed, TimeSpan.FromMinutes(1));
        }

        public void LowMemoryOver()
        {
            // nothing to do here
        }

        public bool HasActive => _active.Count > 0;

        public class Operation
        {
            private readonly TimeSpan _throttleTime = TimeSpan.FromSeconds(1);

            private readonly ThrottledNotification _throttle = new ThrottledNotification();

            public long Id;

            [JsonDeserializationIgnore]
            public Task<IOperationResult> Task;

            [JsonDeserializationIgnore]
            public OperationCancelToken Token;

            public OperationDescription Description;

            public OperationState State;

            [JsonDeserializationIgnore]
            public DocumentDatabase Database;

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
                if (ShouldThrottleMessage(notification) == false)
                {
                    addToNotificationCenter(notification);
                    return;
                }

                // let us throttle changes about the operation progress

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

            private bool ShouldThrottleMessage(OperationChanged notification)
            {
                if (notification.State.Status != OperationStatus.InProgress)
                {
                    return false;
                }

                return true;
            }

            internal void SetCompleted()
            {
                this.Task = null;
            }

            internal bool IsCompleted()
            {
                var task = this.Task;
                return task == null || task.IsCompleted;
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
            public DateTime? EndTime;
            public IOperationDetailedDescription DetailedDescription;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Description)] = Description,
                    [nameof(TaskType)] = TaskType.ToString(),
                    [nameof(StartTime)] = StartTime,
                    [nameof(EndTime)] = EndTime,
                    [nameof(DetailedDescription)] = DetailedDescription?.ToJson()
                };
            }
        }

        public enum OperationType
        {
            [Description("Setup")]
            Setup,

            [Description("Update by Query")]
            UpdateByQuery,

            [Description("Delete by Query")]
            DeleteByQuery,

            [Description("Database export")]
            DatabaseExport,

            [Description("Database import")]
            DatabaseImport,

            [Description("Collection import from CSV")]
            CollectionImportFromCsv,

            [Description("RavenDB Database migration")]
            DatabaseMigrationRavenDb,

            [Description("Database Restore")]
            DatabaseRestore,

            [Description("Database compact")]
            DatabaseCompact,

            [Description("Index compact")]
            IndexCompact,

            [Description("Delete by collection")]
            DeleteByCollection,

            [Description("Bulk Insert")]
            BulkInsert,

            [Description("Replay Transaction Commands")]
            ReplayTransactionCommands,

            [Description("Record Transaction Commands")]
            RecordTransactionCommands,

            [Description("Certificate generation")]
            CertificateGeneration,

            [Description("Migration from v3.x")]
            MigrationFromLegacyData,

            [Description("Database Backup")]
            DatabaseBackup,

            [Description("Migration from SQL")]
            MigrationFromSql,

            [Description("Database Migration")]
            DatabaseMigration,

            [Description("Database Revert")]
            DatabaseRevert,

            [Description("Enforce Revision Configuration")]
            EnforceRevisionConfiguration,
            
            [Description("Debug package")]
            DebugPackage,
        }
    }
}
