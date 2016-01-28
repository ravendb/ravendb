using System;
using System.Collections.Generic;
using Raven.Abstractions.Logging;
using Sparrow.Collections;

namespace Raven.Server.ServerWide.LowMemoryNotification
{
    public class LowMemoryHandlerStatistics
    {
        public string Name { get; set; }
        public long EstimatedUsedMemory { get; set; }
        public string DatabaseName { get; set; }
        public object Metadata { get; set; }
    }
    internal interface ILowMemoryHandler
    {
        void HandleLowMemory();
        void SoftMemoryRelease();
        LowMemoryHandlerStatistics GetStats();
    }

    public class LowMemoryNotification
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(WinLowMemoryNotification));

        private static readonly ConcurrentSet<WeakReference<ILowMemoryHandler>> LowMemoryHandlers = new ConcurrentSet<WeakReference<ILowMemoryHandler>>();

        protected void RunLowMemoryHandlers()
        {
            var inactiveHandlers = new List<WeakReference<ILowMemoryHandler>>();

            foreach (var lowMemoryHandler in LowMemoryHandlers)
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

            inactiveHandlers.ForEach(x => LowMemoryHandlers.TryRemove(x));
        }
    }
}