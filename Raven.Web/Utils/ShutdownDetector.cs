namespace Raven.Web.Utils
{
    using System;
    using System.Reflection;
    using System.Threading;
    using System.Web;
    using System.Web.Hosting;

    internal class ShutdownDetector : IRegisteredObject, IDisposable
    {
        public static ShutdownDetector Instance = new ShutdownDetector();

        private readonly CancellationTokenSource _cts;
        private IDisposable _checkAppPoolTimer;

        private ShutdownDetector()
        {
            _cts = new CancellationTokenSource();
        }

        internal CancellationToken Token
        {
            get { return _cts.Token; }
        }

        internal void Initialize()
        {
            try
            {
                HostingEnvironment.RegisterObject(this);

                // Normally when the AppDomain shuts down IRegisteredObject.Stop gets called, except that
                // ASP.NET waits for requests to end before calling IRegisteredObject.Stop. This can be
                // troublesome for some frameworks like SignalR that keep long running requests alive.
                // These are more aggressive checks to see if the app domain is in the process of being shutdown and
                // we trigger the same cts in that case.
                if (HttpRuntime.UsingIntegratedPipeline)
                {
                    if (RegisterForStopListeningEvent())
                    {
                    }
                    else if (UnsafeIISMethods.CanDetectAppDomainRestart)
                    {
                        // Create a timer for polling when the app pool has been requested for shutdown.

                        _checkAppPoolTimer = new Timer(CheckForAppDomainRestart, state: null, dueTime: TimeSpan.FromSeconds(10), period: TimeSpan.FromSeconds(10));
                    }
                }
            }
            catch (Exception)
            {
            }
        }

        // Note: When we have a compilation that targets .NET 4.5.1, implement IStopListeningRegisteredObject
        // instead of reflecting for HostingEnvironment.StopListening.
        private bool RegisterForStopListeningEvent()
        {
            EventInfo stopEvent = typeof(HostingEnvironment).GetEvent("StopListening");
            if (stopEvent == null)
            {
                return false;
            }
            stopEvent.AddEventHandler(null, new EventHandler(StopListening));
            return true;
        }

        private void StopListening(object sender, EventArgs e)
        {
            Cancel();
        }

        private void CheckForAppDomainRestart(object state)
        {
            if (UnsafeIISMethods.RequestedAppDomainRestart)
                Cancel();
        }

        public void Stop(bool immediate)
        {
            Cancel();
            HostingEnvironment.UnregisterObject(this);
        }

        private void Cancel()
        {
            // Stop the timer as we don't need it anymore
            if (_checkAppPoolTimer != null)
                _checkAppPoolTimer.Dispose();

            // Trigger the cancellation token
            try
            {
                _cts.Cancel(throwOnFirstException: false);
            }
            catch (ObjectDisposedException)
            {
            }
            catch (AggregateException ag)
            {
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _cts.Dispose();

                if (_checkAppPoolTimer != null)
                {
                    _checkAppPoolTimer.Dispose();
                }
            }
        }
    }
}
