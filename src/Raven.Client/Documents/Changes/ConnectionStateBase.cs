using System;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Changes
{
    internal abstract class ConnectionStateBase : IChangesConnectionState
    {
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

        public event Action<Exception> OnError;
        private readonly Func<Task> _onDisconnect;
        private readonly IDatabaseChanges _changes;
        private readonly Func<Task<bool>> _onConnect;
        private int _value;
        private TaskCompletionSource<object> _tcs;

        protected ConnectionStateBase(IDatabaseChanges changes, Func<Task<bool>> onConnect, Func<Task> onDisconnect)
        {
            _changes = changes;
            _onConnect = onConnect;
            _onDisconnect = onDisconnect;
            _value = 0;
            _tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            _changes.ConnectionStatusChanged += OnConnectionStatusChanged;
        }

        private void OnConnectionStatusChanged(object sender, EventArgs eventArgs)
        {
            _semaphore.Wait();

            try
            {
                if (_tcs.Task.Status == TaskStatus.RanToCompletion)
                    _tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

                if (_changes.Connected)
#pragma warning disable 4014
                    Connect();
#pragma warning restore 4014
            }
            finally
            {
                _semaphore.Release();
            }
        }

        private async Task Connect()
        {
            await _semaphore.WaitAsync().ConfigureAwait(false);

            try
            {
                var subscribed = await _onConnect().ConfigureAwait(false);
                if (subscribed)
                    _tcs.TrySetResult(null);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        private async Task Disconnect()
        {
            await _semaphore.WaitAsync().ConfigureAwait(false);

            try
            {
                if (_tcs.Task.Status == TaskStatus.RanToCompletion)
                    _tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

                await _onDisconnect().ConfigureAwait(false);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public void Inc()
        {
            lock (this)
            {
                if (++_value == 1)
#pragma warning disable 4014
                    Connect();
#pragma warning restore 4014
            }
        }

        public void Dec()
        {
            lock (this)
            {
                if (--_value == 0)
#pragma warning disable 4014
                    Disconnect();
#pragma warning restore 4014
            }
        }

        public void Error(Exception e)
        {
            OnError?.Invoke(e);
        }

        public Task EnsureSubscribedNow()
        {
            return _tcs.Task;
        }

        public void Dispose()
        {
            _changes.ConnectionStatusChanged -= OnConnectionStatusChanged;
        }
    }
}
