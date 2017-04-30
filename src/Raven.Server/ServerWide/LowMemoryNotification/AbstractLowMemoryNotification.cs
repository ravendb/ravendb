using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Server.Config;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Platform;

namespace Raven.Server.ServerWide.LowMemoryNotification
{
    public class LowMemoryHandlerStatistics
    {
        public string Name { get; set; }
        public long EstimatedUsedMemory { get; set; }
        public string DatabaseName { get; set; }
        public object Metadata { get; set; }
    }

    public interface ILowMemoryHandler
    {
        void HandleLowMemory();
        void SoftMemoryRelease();
        LowMemoryHandlerStatistics GetStats();
    }

    public abstract class AbstractLowMemoryNotification
    {
        protected readonly Logger Logger;
        private readonly ConcurrentSet<WeakReference<ILowMemoryHandler>> _lowMemoryHandlers = new ConcurrentSet<WeakReference<ILowMemoryHandler>>();

        protected AbstractLowMemoryNotification(string resourceName)
        {
            Logger = LoggingSource.Instance.GetLogger(resourceName, GetType().FullName);
        }

        protected void RunLowMemoryHandlers()
        {
            var inactiveHandlers = new List<WeakReference<ILowMemoryHandler>>();

            foreach (var lowMemoryHandler in _lowMemoryHandlers)
            {
                ILowMemoryHandler handler;
                if (lowMemoryHandler.TryGetTarget(out handler))
                {
                    try
                    {
                        handler.HandleLowMemory();
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Failure to process low memory notification (low memory handler - " + handler + ")", e);
                    }
                }
                else
                    inactiveHandlers.Add(lowMemoryHandler);
            }

            inactiveHandlers.ForEach(x => _lowMemoryHandlers.TryRemove(x));
        }

        protected void ClearInactiveHandlers()
        {
            var inactiveHandlers = new List<WeakReference<ILowMemoryHandler>>();

            foreach (var lowMemoryHandler in _lowMemoryHandlers)
            {
                ILowMemoryHandler handler;
                if (lowMemoryHandler.TryGetTarget(out handler) == false)
                    inactiveHandlers.Add(lowMemoryHandler);
            }

            inactiveHandlers.ForEach(x => _lowMemoryHandlers.TryRemove(x));
        }

        public void RegisterLowMemoryHandler(ILowMemoryHandler handler)
        {
            _lowMemoryHandlers.Add(new WeakReference<ILowMemoryHandler>(handler));
        }

        public abstract void SimulateLowMemoryNotification();

        public static AbstractLowMemoryNotification Instance { get; private set; }

        public static void Initialize(CancellationToken shutdownNotification, RavenConfiguration configuration)
        {
            Instance = PlatformDetails.RunningOnPosix
                ? new PosixLowMemoryNotification(shutdownNotification, configuration) as AbstractLowMemoryNotification
                : new WinLowMemoryNotification(shutdownNotification, configuration);
        }
    }
}