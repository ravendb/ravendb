using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;

namespace Raven.Client.Documents.Changes
{
    internal class DatabaseConnectionState : ConnectionStateBase
    {
        public DatabaseConnectionState(Func<Task> onConnect, Action onDisconnect)
            : base(onConnect, onDisconnect)
        {
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
