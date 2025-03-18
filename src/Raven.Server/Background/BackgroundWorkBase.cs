using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Sparrow.Utils;

namespace Raven.Server.Background
{
    public abstract class BackgroundWorkBase : IDisposable
    {
        private readonly CancellationToken _shutdown;
        protected CancellationTokenSource Cts;
        private Task _currentTask;
        protected readonly RavenLogger Logger;

        protected BackgroundWorkBase(string resourceName, RavenLogger logger, CancellationToken shutdown)
        {
            _shutdown = shutdown;
            Logger = logger;
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
                    //we are disposed, so return a Canceled token ( CancellationToken.None isn't canceled and causes task to freeze)               
                    return CanceledToken.Value;
                }
            }
        }

        private static Lazy<CancellationToken> CanceledToken => new Lazy<CancellationToken>(() =>
           {
               var cts = new CancellationTokenSource();
               cts.Cancel();
               return cts.Token;
           });

        public void Start()
        {
            Debug.Assert(_currentTask == null);
            if (Cts.IsCancellationRequested)
            {
                Cts.Dispose();
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
                {
                    var waitTimeout = TimeSpan.FromSeconds(30);

                    var result = _currentTask.Wait(waitTimeout);

                    if (result == false && Logger.IsInfoEnabled) 
                        Logger.Info($"Background worker didn't manage to stop its task within {waitTimeout}");
                }
            }
            catch (AggregateException e)
            {
                if (e.ExtractSingleInnerException() is OperationCanceledException == false)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Background worker failed to stop", e);
                }
            }

            _currentTask = null;
            Cts.Dispose();
        }

        

        protected async Task WaitOrThrowOperationCanceled(TimeSpan time)
        {
            try
            {
                if (time < TimeSpan.Zero)
                {
                    ThrowOperationCanceledExceptionIfNeeded();
                    return;
                }

                // if cancellation requested then it will throw TaskCancelledException and we stop the work
                await TimeoutManager.WaitFor(time, CancellationToken).ConfigureAwait(false);

                ThrowOperationCanceledExceptionIfNeeded();
            }
            catch (Exception e) when (e is OperationCanceledException == false)
            {
                // can happen if there is an invalid timespan

                if (Logger.IsErrorEnabled && e is ObjectDisposedException == false)
                    Logger.Error($"Error in the background worker when {nameof(WaitOrThrowOperationCanceled)} was called", e);

                throw new OperationCanceledException(); // throw OperationCanceled so we stop the work
            }

            void ThrowOperationCanceledExceptionIfNeeded()
            {
                if (CancellationToken.IsCancellationRequested)
                    throw new OperationCanceledException(); //If we are disposed we need to throw OCE because this is the expected behavior
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
                        if (_shutdown.IsCancellationRequested)
                            return;

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

        public virtual void Dispose()
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
