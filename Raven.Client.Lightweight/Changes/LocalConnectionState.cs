using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database.Util;

namespace Raven.Client.Changes
{
	internal class LocalConnectionState
	{
		private readonly Action onZero;
		private readonly Task task;
		private int value;
		public Task Task
		{
			get { return task; }
		}

		public LocalConnectionState(Action onZero, Task task)
		{
			value = 0;
			this.onZero = onZero;
			this.task = task;
		}

		public void Inc()
		{
			lock (this)
			{
				value++;
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

		public event Action<ReplicationConflictNotification> OnReplicationConflictNotification;

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

		public void Error(Exception e)
		{
			var onOnError = OnError;
			if (onOnError != null)
				onOnError(e);
		}
	}
}
