using System;
using System.Threading.Tasks;
using Raven.Client.Util;

namespace Raven.Client.Documents.Changes
{
    internal abstract class ConnectionStateBase : IChangesConnectionState
    {
        public event Action<Exception> OnError;
        private readonly Func<Task> _onDisconnect;
        private readonly Func<Task> _onConnect;
        private int _value;

        protected ConnectionStateBase(Func<Task> onConnect, Func<Task> onDisconnect)
        {
            _onConnect = onConnect;
            _onDisconnect = onDisconnect;
            _value = 0;
        }

        internal Task OnConnect()
        {
            return _onConnect();
        }

        public void Inc()
        {
            lock (this)
            {
                if (++_value == 1)
                    AsyncHelpers.RunSync(() => _onConnect());
            }
        }

        public void Dec()
        {
            lock (this)
            {
                if (--_value == 0)
                    AsyncHelpers.RunSync(() => _onDisconnect());
            }
        }

        public void Error(Exception e)
        {
            OnError?.Invoke(e);
        }
    }
}
