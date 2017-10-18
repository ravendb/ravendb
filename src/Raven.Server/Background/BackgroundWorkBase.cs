using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Background
{
    public abstract class BackgroundWorkBase : IDisposable
    {
        private readonly CancellationToken _shutdown;
        protected CancellationTokenSource Cts;
        private Task _currentTask;
        protected readonly Logger Logger;

        protected BackgroundWorkBase(string resourceName, CancellationToken shutdown)
        {
            _shutdown = shutdown;
            Logger = LoggingSource.Instance.GetLogger(resourceName, GetType().FullName);
            Cts = CancellationTokenSource.CreateLinkedTokenSource(_shutdown);
        }

        protected CancellationToken CancellationToken
        {
            get
            {
                try
                {
                    return Cts.Token;
                }
                catch (ObjectDisposedException)
                {
                    //we are disposed, so return "null" token                   
                    return CancellationToken.None;
                }
            }
        }

        public void Start()
        {
            Debug.Assert(_currentTask == null);
            if (Cts.IsCancellationRequested)
            {
                Cts = CancellationTokenSource.CreateLinkedTokenSource(_shutdown);
            }

            _currentTask = Task.Run(Run, CancellationToken);
        }

        public void Stop()
        {
            if (Cts.IsCancellationRequested)
                return;

            Debug.Assert(_currentTask != null);

            Cts.Cancel();

            try
            {
                if (_currentTask.Status == TaskStatus.Running)
                    _currentTask.Wait(Cts.Token);
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
            Cts.Dispose();
        }

        protected async Task WaitOrThrowOperationCanceled(TimeSpan time)
        {
            try
            {
                // if cancellation requested then it will throw TaskCancelledException and we stop the work
                await TimeoutManager.WaitFor(time, CancellationToken).ConfigureAwait(false); 
            }
            catch (Exception e) when (e is OperationCanceledException == false)
            {
                // can happen if there is an invalid timespan

                if (Logger.IsOperationsEnabled && e is ObjectDisposedException == false)
                    Logger.Operations($"Error in the background worker when {nameof(WaitOrThrowOperationCanceled)} was called", e);

                throw new OperationCanceledException(); // throw OperationCanceled so we stop the work
            }
        }

        protected async Task Run()
        {
            InitializeWork();

            try
            {
                while (Cts.IsCancellationRequested == false)
                {
                    try
                    {
                        await DoWork().ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Error in the background worker", e);
                    }
                }
            }
            catch (ObjectDisposedException)
            {
                //if we are disposed, don't care
            }
        }

        protected virtual void InitializeWork()
        {
        }

        protected abstract Task DoWork();

        public void Dispose()
        {
            try
            {
                Stop();
                Cts.Dispose();
            }
            catch (ObjectDisposedException) //precaution, shouldn't happen
            {
                //don't care, we are disposing...
            }
        }
    }
}
