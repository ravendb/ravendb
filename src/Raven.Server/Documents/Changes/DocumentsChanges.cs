using System;
using Raven.Client.Documents.Changes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Changes
{
    public sealed class DocumentsChanges : DocumentsChangesBase<ChangesClientConnection, DocumentsOperationContext>
    {
        public event Action<DocumentChange> OnDocumentChange;

        public event Action<CounterChange> OnCounterChange;

        public event Action<TimeSeriesChange> OnTimeSeriesChange;

        public event Action<IndexChange> OnIndexChange;

        public void RaiseNotifications(IndexChange indexChange)
        {
            OnIndexChange?.Invoke(indexChange);

            if (HasConnections == false)
                return;

            foreach (var connection in Connections)
                connection.Value.SendIndexChanges(indexChange);
        }

        public void RaiseInternalDocumentChangeNotification(DocumentChange documentChange)
        {
            OnDocumentChange?.Invoke(documentChange);
        }

        public void SendDocumentChangeToConnections(DocumentChange documentChange)
        {
            foreach (var connection in Connections)
            {
                connection.Value.SendDocumentChanges(documentChange);
            }
        }

        public void RaiseInternalCounterChangeNotification(CounterChange counterChange)
        {
            OnCounterChange?.Invoke(counterChange);
        }

        public void SendCounterChangeToConnections(CounterChange counterChange)
        {
            foreach (var connection in Connections)
            {
                connection.Value.SendCounterChanges(counterChange);
            }
        }

        public void RaiseInternalTimeSeriesChangeNotification(TimeSeriesChange timeSeriesChange)
        {
            OnTimeSeriesChange?.Invoke(timeSeriesChange);
        }

        public void SendTimeSeriesChangeToConnections(TimeSeriesChange timeSeriesChange)
        {
            foreach (var connection in Connections)
            {
                connection.Value.SendTimeSeriesChanges(timeSeriesChange);
            }
        }
    }
}
