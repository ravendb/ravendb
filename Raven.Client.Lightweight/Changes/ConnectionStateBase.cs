using System;
using System.Threading.Tasks;

namespace Raven.Client.Changes
{
    public abstract class ConnectionStateBase : IChangesConnectionState
	{
		public event Action<Exception> OnError;
		private readonly Action onZero;
	    private int value;

		public Task Task { get; private set; }

		protected ConnectionStateBase(Action onZero, Task task)
		{
			value = 0;
			this.onZero = onZero;
			Task = task;
		}

		protected abstract Task EnsureConnection();

		public void Inc()
		{
			lock (this)
			{
				if (++value == 1)
					Task = EnsureConnection();
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

		public void Error(Exception e)
		{
			var onOnError = OnError;
			if (onOnError != null)
				onOnError(e);
		}
	}
}
