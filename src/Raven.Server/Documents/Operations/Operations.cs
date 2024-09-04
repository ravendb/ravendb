using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Changes;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Sparrow.Platform;

namespace Raven.Server.Documents.Operations
{
    public sealed class ServerOperations : Operations
    {
        public ServerOperations(ServerStore serverStore, OperationsStorage operationsStorage)
            : base(null, operationsStorage, serverStore.NotificationCenter, null, PlatformDetails.Is32Bits || serverStore.Configuration.Storage.ForceUsing32BitsPager
                ? TimeSpan.FromHours(12)
                : TimeSpan.FromDays(2))
        {
        }
    }

    public sealed class DatabaseOperations : Operations
    {
        public DatabaseOperations(DocumentDatabase database)
            : base(database.Name, database.ConfigurationStorage.OperationsStorage, database.NotificationCenter, database.Changes, database.Is32Bits ? TimeSpan.FromHours(12) : TimeSpan.FromDays(2))
        {
        }
    }

    public abstract class Operations : AbstractOperations<Operation>
    {
        private readonly string _databaseName;
        private readonly OperationsStorage _operationsStorage;
        private readonly AbstractNotificationCenter _notificationCenter;

        protected Operations(string databaseName,
            OperationsStorage operationsStorage,
            AbstractNotificationCenter notificationCenter,
            DocumentsChanges changes,
            TimeSpan maxCompletedTaskLifeTime)
            : base(changes, maxCompletedTaskLifeTime)
        {
            _databaseName = databaseName;
            _operationsStorage = operationsStorage;
            _notificationCenter = notificationCenter;
        }

        public override Task<IOperationResult> AddLocalOperation(
            long id,
            OperationType operationType,
            string description,
            IOperationDetailedDescription detailedDescription,
            Func<Action<IOperationProgress>, Task<IOperationResult>> taskFactory,
            OperationCancelToken token = null,
            string databaseName = null)
        {
            var operation = CreateOperationInstance(id, _databaseName ?? databaseName, operationType, description, detailedDescription, token);

            return AddOperationInternalAsync(operation, taskFactory);
        }

        public override long GetNextOperationId() => _operationsStorage.GetNextOperationId();

        protected override void RaiseNotifications(OperationStatusChange change, AbstractOperation operation)
        {
            var operationChanged = OperationChanged.Create(_databaseName, change.OperationId, operation.Description, change.State, operation.Killable);

            operation.NotifyCenter(operationChanged, x => _notificationCenter.Add(x));

            base.RaiseNotifications(change, operation);
        }
    }
}
