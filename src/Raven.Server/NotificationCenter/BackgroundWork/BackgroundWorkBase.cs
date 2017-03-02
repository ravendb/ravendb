using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter.BackgroundWork
{
    public abstract class BackgroundWorkBase : IDisposable
    {
        private readonly CancellationToken _shutdown;
        private CancellationTokenSource _cts;
        private Task _currentTask;

        protected readonly Logger Logger;

        protected BackgroundWorkBase(string resourceName, CancellationToken shutdown)
        {
            _shutdown = shutdown;
            Logger = LoggingSource.Instance.GetLogger<NotificationCenter>(resourceName);
        }

        protected CancellationToken CancellationToken => _cts.Token;

        public void Start()
        {
            Debug.Assert(_currentTask == null);

            _cts = CancellationTokenSource.CreateLinkedTokenSource(_shutdown);

            _currentTask = Task.Run(Run, CancellationToken);
        }

        public void Stop()
        {
            if (_cts == null || _cts.IsCancellationRequested)
                return;

            Debug.Assert(_currentTask != null);

            _cts.Cancel();

            try
            {
                if (_currentTask.Status == TaskStatus.Running)
                    _currentTask.Wait();
            }
            catch (AggregateException e)
            {
                if (e.InnerException is OperationCanceledException == false)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Background worker of the notification center failed to stop", e);
                }
            }

            _currentTask = null;
            _cts.Dispose();
            _cts = null;
        }

        protected abstract Task Run();

        public void Dispose()
        {
            Stop();
            _cts?.Dispose();
        }
    }
}