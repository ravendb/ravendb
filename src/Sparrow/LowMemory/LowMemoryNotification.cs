using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Sparrow.LowMemory
{
    public class LowMemoryFlag
    {
        public LowMemoryFlag()
        {
            if (LowMemoryNotification.Instance != null)
                LowMemoryState = LowMemoryNotification.Instance.LowMemoryState ? 1 : 0;
        }

        public int LowMemoryState;

        public static LowMemoryFlag None = new LowMemoryFlag();
    }

    public class LowMemoryHandlerStatistics
    {
        public string Name { get; set; }
        public long EstimatedUsedMemory { get; set; }
        public string DatabaseName { get; set; }
        public object Metadata { get; set; }
    }

    public interface ILowMemoryHandler
    {
        void LowMemory();
        void LowMemoryOver();
    }

    public class LowMemoryNotification
    {
        private readonly Logger _logger;
        private readonly ConcurrentSet<WeakReference<ILowMemoryHandler>> _lowMemoryHandlers = new ConcurrentSet<WeakReference<ILowMemoryHandler>>();

        private void RunLowMemoryHandlers(bool isLowMemory)
        {
            var inactiveHandlers = new List<WeakReference<ILowMemoryHandler>>();

            foreach (var lowMemoryHandler in _lowMemoryHandlers)
            {
                ILowMemoryHandler handler;
                if (lowMemoryHandler.TryGetTarget(out handler))
                {
                    try
                    {
                        if (isLowMemory)
                            handler.LowMemory();
                        else
                            handler.LowMemoryOver();
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

        private void ClearInactiveHandlers()
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

        public static LowMemoryNotification Instance { get; private set; }
        public bool LowMemoryState { get; set; }

        public static void Initialize(CancellationToken shutdownNotification, long lowMemoryThreshold, double physicalRatioForLowMemDetection)
        {
            Instance = new LowMemoryNotification(shutdownNotification, lowMemoryThreshold, physicalRatioForLowMemDetection);
        }

        private readonly long _lowMemoryThreshold;
        private readonly ManualResetEvent _simulatedLowMemory = new ManualResetEvent(false);
        private readonly ManualResetEvent _shutdownRequested = new ManualResetEvent(false);
        private readonly ManualResetEvent _warnAllocation = new ManualResetEvent(false);
        private readonly double _physicalRatioForLowMemDetection;

        public LowMemoryNotification(CancellationToken shutdownNotification, long lowMemoryThreshold, double physicalRatioForLowMemDetection)
        {
            _logger = LoggingSource.Instance.GetLogger<LowMemoryNotification>("Server");

            shutdownNotification.Register(() => _shutdownRequested.Set());
            _lowMemoryThreshold = lowMemoryThreshold;
            _physicalRatioForLowMemDetection = physicalRatioForLowMemDetection;
            new Thread(MonitorMemoryUsage)
            {
                IsBackground = true,
                Name = "Low memory notification thread"
            }.Start();
        }

        private void MonitorMemoryUsage()
        {
            int clearInactiveHandlersCounter = 0;
            var memoryAvailableHandles = new WaitHandle[] { _simulatedLowMemory, _shutdownRequested };
            var paranoidModeHandles = new WaitHandle[] { _simulatedLowMemory, _shutdownRequested, _warnAllocation };
            var timeout = 5 * 1000;
            long totalUnmanagedAllocations = 0;
            while (true)
            {
                var handles = MemoryInformation.GetMemoryInfo().AvailableMemory.GetValue(SizeUnit.Bytes) < _lowMemoryThreshold * 2 ?
                    paranoidModeHandles :
                    memoryAvailableHandles;
                switch (WaitHandle.WaitAny(handles, timeout))
                {
                    case WaitHandle.WaitTimeout:
                        if (++clearInactiveHandlersCounter > 60) // 5 minutes == WaitAny 5 Secs * 60
                        {
                            clearInactiveHandlersCounter = 0;
                            ClearInactiveHandlers();
                        }
                        var memInfo = MemoryInformation.GetMemoryInfo();
                        foreach (var stats in NativeMemory.ThreadAllocations.Values
                            .Where(x => x.ThreadInstance.IsAlive)
                            .GroupBy(x => x.Name)
                            .OrderByDescending(x => x.Sum(y => y.Allocations)))
                        {
                            var unmanagedAllocations = stats.Sum(x => x.Allocations);
                            totalUnmanagedAllocations += unmanagedAllocations;
                        }

                        var availableMem = memInfo.AvailableMemory.GetValue(SizeUnit.Bytes);
                        if (availableMem < _lowMemoryThreshold && 
                            totalUnmanagedAllocations > memInfo.TotalPhysicalMemory.GetValue(SizeUnit.Bytes) * _physicalRatioForLowMemDetection)
                        {
                            if (LowMemoryState == false && _logger.IsInfoEnabled)
                                _logger.Info("Low memory detected, will try to reduce memory usage...");
                            LowMemoryState = true;
                            clearInactiveHandlersCounter = 0;
                            RunLowMemoryHandlers(true);
                            timeout = 500;
                        }
                        else
                        {
                            if (LowMemoryState && _logger.IsInfoEnabled)
                                _logger.Info("Back to normal memory usage detected");
                            LowMemoryState = false;
                            RunLowMemoryHandlers(false);
                            timeout = availableMem < _lowMemoryThreshold * 2 ? 1000 : 5000;
                        }
                        break;
                    case 0:
                        _simulatedLowMemory.Reset();
                        LowMemoryState = !LowMemoryState;
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Simulating : " + (LowMemoryState ? "Low memory event" : "Back to normal memory usage"));
                        RunLowMemoryHandlers(LowMemoryState);
                        break;
                    case 2: // check allocations
                        _warnAllocation.Reset();
                        goto case WaitHandle.WaitTimeout;
                    case 1: // shutdown requested
                        return;
                    default:
                        return;
                }
            }
        }

        public static void NotifyAllocationPending()
        {
            Instance?._warnAllocation.Set();
        }

        public void SimulateLowMemoryNotification()
        {
            _simulatedLowMemory.Set();
        }
    }
}

