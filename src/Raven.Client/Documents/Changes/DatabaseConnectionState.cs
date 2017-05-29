using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;

namespace Raven.Client.Documents.Changes
{
    internal class DatabaseConnectionState : ConnectionStateBase
    {
        public DatabaseConnectionState(IDatabaseChanges changes, Func<Task<bool>> onConnect, Func<Task> onDisconnect)
            : base(changes, onConnect, onDisconnect)
        {
        }

        public event Action<DocumentChange> OnDocumentChangeNotification;

        public event Action<IndexChange> OnIndexChangeNotification;

        public event Action<TransformerChange> OnTransformerChangeNotification;

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

        public void Send(OperationStatusChange operationStatusChange)
        {
            OnOperationStatusChangeNotification?.Invoke(operationStatusChange);
        }
    }
}
