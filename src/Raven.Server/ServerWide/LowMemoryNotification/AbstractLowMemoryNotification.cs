using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Logging;
using Raven.Server.Config;
using Sparrow.Collections;
using Sparrow;
using Sparrow.Logging;

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
        private static Logger _logger;
        private readonly ConcurrentSet<WeakReference<ILowMemoryHandler>> _lowMemoryHandlers = new ConcurrentSet<WeakReference<ILowMemoryHandler>>();
        private static AbstractLowMemoryNotification _instance;

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
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Failure to process low memory notification (low memory handler - " + handler + ")", e);
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

        public static AbstractLowMemoryNotification Instance
        {
            get { return _instance; }
        }

        public static void Initialize(CancellationToken shutdownNotification, RavenConfiguration configuration)
        {
            _instance = Platform.RunningOnPosix
                ? new PosixLowMemoryNotification(shutdownNotification, configuration) as AbstractLowMemoryNotification
                : new WinLowMemoryNotification(shutdownNotification, configuration);
            _logger = LoggingSource.Instance.GetLogger<AbstractLowMemoryNotification>("Raven/Server");
        }
    }
}