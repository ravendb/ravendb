using System;
using System.Collections.Generic;
using Raven.Abstractions.Logging;
using Sparrow.Collections;
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
        private static readonly ILog Log = LogManager.GetLogger(typeof(WinLowMemoryNotification));

        private readonly ConcurrentSet<WeakReference<ILowMemoryHandler>> _lowMemoryHandlers = new ConcurrentSet<WeakReference<ILowMemoryHandler>>();

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
                        Log.Error("Failure to process low memory notification (low memory handler - " + handler + ")", e);
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
    }
}