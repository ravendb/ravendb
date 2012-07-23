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
		private int value = 1;
		private readonly ConcurrentSet<Task<IDisposable>> toDispose = new ConcurrentSet<Task<IDisposable>>();
		public Task Task
		{
			get { return task; }
		}

		public LocalConnectionState(Action onZero, Task task)
		{
			this.onZero = onZero;
			this.task = task;
		}

		public void Inc()
		{
			Interlocked.Increment(ref value);
		}

		public void Dec()
		{
			if (Interlocked.Decrement(ref value) == 0)
			{
				Dispose();
			}
		}

		public void Add(Task<IDisposable> disposableTask)
		{
			if (value == 0)
			{
				disposableTask.ContinueWith(_ => { using (_.Result) { } });
				return;
			}
			toDispose.Add(disposableTask);
		}

		public void Dispose()
		{
			foreach (var disposableTask in toDispose)
			{
				disposableTask.ContinueWith(_ => { using (_.Result) { } });
			}
			onZero();
		}

		public event Action<DocumentChangeNotification> OnDocumentChangeNotification = delegate { };

		public event Action<IndexChangeNotification> OnIndexChangeNotification = delegate { };

		public Action<Exception> OnError = delegate { };

		public void Send(DocumentChangeNotification documentChangeNotification)
		{
			OnDocumentChangeNotification(documentChangeNotification);
		}

		public void Send(IndexChangeNotification indexChangeNotification)
		{
			OnIndexChangeNotification(indexChangeNotification);
		}

		public void Error(Exception e)
		{
			OnError(e);	
		}
	}
}