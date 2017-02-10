using System;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
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

        public event Action<DocumentChange> OnDocumentChangeNotification = delegate { };

        public event Action<BulkInsertChange> OnBulkInsertChangeNotification = delegate { };

        public event Action<IndexChange> OnIndexChangeNotification;

        public event Action<TransformerChange> OnTransformerChangeNotification;

        public event Action<ReplicationConflictChange> OnReplicationConflictNotification;

        public event Action<DataSubscriptionChange> OnDataSubscriptionNotification;

        public event Action<OperationStatusChange> OnOperationStatusChangeNotification;

        public void Send(DocumentChange documentChange)
        {
            OnDocumentChangeNotification?.Invoke(documentChange);
        }

        public void Send(IndexChange indexChange)
        {
            OnIndexChangeNotification?.Invoke(indexChange);
        }

        public void Send(TransformerChange transformerChange)
        {
            OnTransformerChangeNotification?.Invoke(transformerChange);
        }

        public void Send(ReplicationConflictChange replicationConflictChange)
        {
            OnReplicationConflictNotification?.Invoke(replicationConflictChange);
        }

        public void Send(BulkInsertChange bulkInsertChange)
        {
            OnBulkInsertChangeNotification?.Invoke(bulkInsertChange);

            Send((DocumentChange)bulkInsertChange);
        }

        public void Send(DataSubscriptionChange dataSubscriptionChange)
        {
            OnDataSubscriptionNotification?.Invoke(dataSubscriptionChange);
        }

        public void Send(OperationStatusChange operationStatusChange)
        {
            OnOperationStatusChangeNotification?.Invoke(operationStatusChange);
        }
    }
}
