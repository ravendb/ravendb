using System;
using System.Collections.Generic;
using System.Threading;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Sparrow.LowMemory
{
    public class LowMemoryFlag
    {
        public int LowMemoryState;

        public static LowMemoryFlag None = new LowMemoryFlag();
    }

    public interface ILowMemoryHandler
    {
        void LowMemory();
        void LowMemoryOver();
    }

    public class LowMemoryNotification
    {
        private const string NotificationThreadName = "Low memory notification thread";
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

        public static readonly LowMemoryNotification Instance = new LowMemoryNotification(1024 * 1024 * 256, 0.1);

        public bool LowMemoryState { get; set; }

        public static void Initialize(CancellationToken shutdownNotification, long lowMemoryThreshold, double physicalRatioForLowMemDetection)
        {
            Instance._lowMemoryThreshold = lowMemoryThreshold;
            Instance._physicalRatioForLowMemDetection = physicalRatioForLowMemDetection;

            shutdownNotification.Register(() => Instance._shutdownRequested.Set());
        }

        private long _lowMemoryThreshold;
        private double _physicalRatioForLowMemDetection;
        private readonly ManualResetEvent _simulatedLowMemory = new ManualResetEvent(false);
        private readonly ManualResetEvent _shutdownRequested = new ManualResetEvent(false);
        private readonly ManualResetEvent _warnAllocation = new ManualResetEvent(false);

        public LowMemoryNotification(long lowMemoryThreshold, double physicalRatioForLowMemDetection)
        {
            _logger = LoggingSource.Instance.GetLogger<LowMemoryNotification>("Server");

            _lowMemoryThreshold = lowMemoryThreshold;
            _physicalRatioForLowMemDetection = physicalRatioForLowMemDetection;
            var thread = new Thread(MonitorMemoryUsage)
            {
                IsBackground = true,
                Name = NotificationThreadName
            };
            
            thread.Start();
        }

        private void MonitorMemoryUsage()
        {
            NativeMemory.EnsureRegistered();
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
                        foreach (var stats in NativeMemory.ThreadAllocations.Values)
                        {
                            if (stats.ThreadInstance.IsAlive == false)
                                continue;

                            totalUnmanagedAllocations += stats.Allocations;
                        }

                        var memInfo = MemoryInformation.GetMemoryInfo();

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

