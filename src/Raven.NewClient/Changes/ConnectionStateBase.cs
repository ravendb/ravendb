using System;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.Changes
{
    public abstract class ConnectionStateBase : IChangesConnectionState
    {
        public event Action<Exception> OnError;
        private readonly Func<Task> _disconnectAction;
        private int _value;

        public Task Task { get; private set; }

        protected ConnectionStateBase(Func<Task> disconnectAction, Task task)
        {
            _value = 0;
            _disconnectAction = disconnectAction;
            Task = task;
        }

        protected abstract Task EnsureConnection();

        public void Inc()
        {
            lock (this)
            {
                if (++_value == 1)
                    Task = EnsureConnection();
            }
        }

        public void Dec()
        {
            lock(this)
            {
                if(--_value == 0)
                    _disconnectAction();
            }
        }

        public void Error(Exception e)
        {
            OnError?.Invoke(e);
        }
    }
}
