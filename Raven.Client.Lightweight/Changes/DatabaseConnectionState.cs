using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Client.Changes
{
	public class DatabaseConnectionState : ConnectionStateBase
	{
		private readonly Func<DatabaseConnectionState, Task> ensureConnection;

		public DatabaseConnectionState(Action onZero, Func<DatabaseConnectionState, Task> ensureConnection, Task task)
			: base(onZero, task)
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
	}
}
