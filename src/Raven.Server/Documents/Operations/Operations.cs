using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Raven.Server.Documents.Changes;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Sparrow.LowMemory;

namespace Raven.Server.Documents.Operations
{
    public class Operations : AbstractOperations<Operation>, ILowMemoryHandler
    {
        private readonly string _name;
        private readonly OperationsStorage _operationsStorage;
        private readonly NotificationCenter.NotificationCenter _notificationCenter;
        private readonly TimeSpan _maxCompletedTaskLifeTime;

        public Operations(string name,
            OperationsStorage operationsStorage,
            NotificationCenter.NotificationCenter notificationCenter,
            DocumentsChanges changes,
            TimeSpan maxCompletedTaskLifeTime)
            : base(changes)
        {
            _name = name;
            _operationsStorage = operationsStorage;
            _notificationCenter = notificationCenter;
            _maxCompletedTaskLifeTime = maxCompletedTaskLifeTime;
            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
        }

        public Task<IOperationResult> AddOperation(
            string databaseName,
            string description,
            OperationType operationType,
            Func<Action<IOperationProgress>, Task<IOperationResult>> taskFactory,
            long id,
            IOperationDetailedDescription detailedDescription = null,
            OperationCancelToken token = null)
        {
            var operation = CreateOperationInstance(id, databaseName, operationType, description, detailedDescription, token);

            return AddOperationInternalAsync(operation, taskFactory);
        }

        public override long GetNextOperationId() => _operationsStorage.GetNextOperationId();

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            // cleanup operations older than 1 minute only
            // Client API might still be waiting for the status
            CleanupOperationsInternal(Completed, TimeSpan.FromMinutes(1));
        }

        public void LowMemoryOver()
        {
            // nothing to do here
        }

        protected override void RaiseNotifications(OperationStatusChange change, Operation operation)
        {
            var operationChanged = OperationChanged.Create(_name, change.OperationId, operation.Description, change.State, operation.Killable);

            operation.NotifyCenter(operationChanged, x => _notificationCenter.Add(x));

            base.RaiseNotifications(change, operation);
        }

        internal void CleanupOperations()
        {
            CleanupOperationsInternal(Completed, _maxCompletedTaskLifeTime);
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
    }
}
