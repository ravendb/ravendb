using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;

namespace Raven.Client.Documents.Changes
{
    internal class DatabaseConnectionState : DatabaseConnectionStateBase, IChangesConnectionState<DocumentChange>, IChangesConnectionState<IndexChange>, IChangesConnectionState<OperationStatusChange>, IChangesConnectionState<CounterChange>, IChangesConnectionState<TimeSeriesChange>
    {
        private event Action<DocumentChange> OnDocumentChangeNotification;

        private event Action<CounterChange> OnCounterChangeNotification;

        private event Action<IndexChange> OnIndexChangeNotification;

        private event Action<OperationStatusChange> OnOperationStatusChangeNotification;

        private event Action<TimeSeriesChange> OnTimeSeriesChangeNotification;

        public DatabaseConnectionState(Func<Task> onConnect, Func<Task> onDisconnect) 
            : base(onConnect, onDisconnect)
        {
        }

        public void Send(DocumentChange documentChange)
        {
            OnDocumentChangeNotification?.Invoke(documentChange);
        }

        public void Send(CounterChange counterChange)
        {
            OnCounterChangeNotification?.Invoke(counterChange);
        }

        public void Send(TimeSeriesChange timeSeriesChange)
        {
            OnTimeSeriesChangeNotification?.Invoke(timeSeriesChange);
        }

        public void Send(IndexChange indexChange)
        {
            OnIndexChangeNotification?.Invoke(indexChange);
        }

        public void Send(OperationStatusChange operationStatusChange)
        {
            OnOperationStatusChangeNotification?.Invoke(operationStatusChange);
        }

        event Action<TimeSeriesChange> IChangesConnectionState<TimeSeriesChange>.OnChangeNotification
        {
            add => OnTimeSeriesChangeNotification += value;
            remove => OnTimeSeriesChangeNotification -= value;
        }

        event Action<CounterChange> IChangesConnectionState<CounterChange>.OnChangeNotification
        {
            add => OnCounterChangeNotification += value;
            remove => OnCounterChangeNotification -= value;
        }

        event Action<OperationStatusChange> IChangesConnectionState<OperationStatusChange>.OnChangeNotification
        {
            add => OnOperationStatusChangeNotification += value;
            remove => OnOperationStatusChangeNotification -= value;
        }

        event Action<IndexChange> IChangesConnectionState<IndexChange>.OnChangeNotification
        {
            add => OnIndexChangeNotification += value;
            remove => OnIndexChangeNotification -= value;
        }

        event Action<DocumentChange> IChangesConnectionState<DocumentChange>.OnChangeNotification
        {
            add => OnDocumentChangeNotification += value;
            remove => OnDocumentChangeNotification -= value;
        }

        public override void Dispose()
        {
            base.Dispose();

            OnDocumentChangeNotification = null;
            OnCounterChangeNotification = null;
            OnTimeSeriesChangeNotification = null;
            OnIndexChangeNotification = null;
            OnOperationStatusChangeNotification = null;
        }
    }
}
