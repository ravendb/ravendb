using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database.Util;

namespace Raven.Client.Changes
{
    public class DatabaseConnectionState : IChangesConnectionState
    {
        private readonly Action onZero;
        private readonly Func<DatabaseConnectionState, Task> ensureConnection;
        private Task task;
        private int value;
        public Task Task
        {
            get { return task; }
        }

        public DatabaseConnectionState(Action onZero, Func<DatabaseConnectionState, Task> ensureConnection, Task task)
        {
            value = 0;
            this.onZero = onZero;
            this.ensureConnection = ensureConnection;
            this.task = task;
        }

        public void Inc()
        {
            lock (this)
            {
                if (++value == 1)
                    task = ensureConnection(this);
            }

        }

        public void Dec()
        {
            lock(this)
            {
                if(--value == 0)
                    onZero();
            }
        }

        public event Action<DocumentChangeNotification> OnDocumentChangeNotification = delegate { };

        public event Action<BulkInsertChangeNotification> OnBulkInsertChangeNotification = delegate { };

        public event Action<IndexChangeNotification> OnIndexChangeNotification;

        public event Action<TransformerChangeNotification> OnTransformerChangeNotification;

        public event Action<ReplicationConflictNotification> OnReplicationConflictNotification;

        public event Action<DataSubscriptionChangeNotification> OnDataSubscriptionNotification;

        public event Action<Exception> OnError;

        public void Send(DocumentChangeNotification documentChangeNotification)
        {
            var onOnDocumentChangeNotification = OnDocumentChangeNotification;
            if (onOnDocumentChangeNotification != null)
                onOnDocumentChangeNotification(documentChangeNotification);
        }

        public void Send(IndexChangeNotification indexChangeNotification)
        {
            var onOnIndexChangeNotification = OnIndexChangeNotification;
            if (onOnIndexChangeNotification != null)
                onOnIndexChangeNotification(indexChangeNotification);
        }

        public void Send(TransformerChangeNotification transformerChangeNotification)
        {
            var onOnTransformerChangeNotification = OnTransformerChangeNotification;
            if (onOnTransformerChangeNotification != null)
            {
                onOnTransformerChangeNotification(transformerChangeNotification);
            }
        }

        public void Send(ReplicationConflictNotification replicationConflictNotification)
        {
            var onOnReplicationConflictNotification = OnReplicationConflictNotification;
            if (onOnReplicationConflictNotification != null)
                onOnReplicationConflictNotification(replicationConflictNotification);
        }

        public void Send(BulkInsertChangeNotification bulkInsertChangeNotification)
        {
            var onOnBulkInsertChangeNotification = OnBulkInsertChangeNotification;
            if (onOnBulkInsertChangeNotification != null)
                onOnBulkInsertChangeNotification(bulkInsertChangeNotification);

            Send((DocumentChangeNotification)bulkInsertChangeNotification);
        }

        public void Send(DataSubscriptionChangeNotification dataSubscriptionChangeNotification)
        {
            var onOnDataSubscriptionChangeNotification = OnDataSubscriptionNotification;
            if (onOnDataSubscriptionChangeNotification != null)
                onOnDataSubscriptionChangeNotification(dataSubscriptionChangeNotification);
        }

        public void Error(Exception e)
        {
            var onOnError = OnError;
            if (onOnError != null)
                onOnError(e);
        }
    }
}
