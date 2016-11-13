using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.NewClient.Client.Data;

namespace Raven.NewClient.Client.Changes
{
    public class DatabaseConnectionState : ConnectionStateBase
    {
        private readonly Func<DatabaseConnectionState, Task> ensureConnection;

        public DatabaseConnectionState(Func<Task> disconnectAction, Func<DatabaseConnectionState, Task> ensureConnection, Task task)
            : base(disconnectAction, task)
        {
            this.ensureConnection = ensureConnection;
        }

        protected override Task EnsureConnection()
        {
            return ensureConnection(this);
        }

        public event Action<DocumentChangeNotification> OnDocumentChangeNotification = delegate { };

        public event Action<BulkInsertChangeNotification> OnBulkInsertChangeNotification = delegate { };

        public event Action<IndexChangeNotification> OnIndexChangeNotification;

        public event Action<TransformerChangeNotification> OnTransformerChangeNotification;

        public event Action<ReplicationConflictNotification> OnReplicationConflictNotification;

        public event Action<DataSubscriptionChangeNotification> OnDataSubscriptionNotification;

        public event Action<OperationStatusChangeNotification> OnOperationStatusChangeNotification;

        public void Send(DocumentChangeNotification documentChangeNotification)
        {
            OnDocumentChangeNotification?.Invoke(documentChangeNotification);
        }

        public void Send(IndexChangeNotification indexChangeNotification)
        {
            OnIndexChangeNotification?.Invoke(indexChangeNotification);
        }

        public void Send(TransformerChangeNotification transformerChangeNotification)
        {
            OnTransformerChangeNotification?.Invoke(transformerChangeNotification);
        }

        public void Send(ReplicationConflictNotification replicationConflictNotification)
        {
            OnReplicationConflictNotification?.Invoke(replicationConflictNotification);
        }

        public void Send(BulkInsertChangeNotification bulkInsertChangeNotification)
        {
            OnBulkInsertChangeNotification?.Invoke(bulkInsertChangeNotification);

            Send((DocumentChangeNotification)bulkInsertChangeNotification);
        }

        public void Send(DataSubscriptionChangeNotification dataSubscriptionChangeNotification)
        {
            OnDataSubscriptionNotification?.Invoke(dataSubscriptionChangeNotification);
        }

        public void Send(OperationStatusChangeNotification operationStatusChangeNotification)
        {
            OnOperationStatusChangeNotification?.Invoke(operationStatusChangeNotification);
        }
    }
}
