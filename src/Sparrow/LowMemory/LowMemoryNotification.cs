using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Utils;

namespace Sparrow.LowMemory
{
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

        public enum LowMemReason
        {
            None = 0,
            LowMemOnTimeoutChk,
            BackToNormal,
            BackToNormalSimulation,
            LowMemStateSimulation,
            BackToNormalHandler,
            LowMemHandler
        }

        public class LowMemEventDetails
        {
            public LowMemReason Reason;
            public long FreeMem;
            public DateTime Time;
            public long PhysicalMem { get; set; }
            public long TotalUnmanaged { get; set; }
            public long LowMemThreshold { get; set; }
        }

        public LowMemEventDetails[] LowMemEventDetailsStack = new LowMemEventDetails[256];
        private int _lowMemEventDetailsIndex;

        private void RunLowMemoryHandlers(bool isLowMemory)
        {
            var inactiveHandlers = new List<WeakReference<ILowMemoryHandler>>();

            foreach (var lowMemoryHandler in _lowMemoryHandlers)
            {
                if (lowMemoryHandler.TryGetTarget(out var handler))
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
                if (lowMemoryHandler.TryGetTarget(out _) == false)
                    inactiveHandlers.Add(lowMemoryHandler);
            }

            inactiveHandlers.ForEach(x => _lowMemoryHandlers.TryRemove(x));
        }

        public void RegisterLowMemoryHandler(ILowMemoryHandler handler)
        {
            _lowMemoryHandlers.Add(new WeakReference<ILowMemoryHandler>(handler));
        }

        public static readonly LowMemoryNotification Instance = new LowMemoryNotification(1024 * 1024 * 256);

        public bool LowMemoryState { get; set; }

        public static void Initialize(CancellationToken shutdownNotification, long lowMemoryThreshold)
        {
            Instance._lowMemoryThreshold = lowMemoryThreshold;

            shutdownNotification.Register(() => Instance._shutdownRequested.Set());
        }

        private long _lowMemoryThreshold;
        private readonly ManualResetEvent _simulatedLowMemory = new ManualResetEvent(false);
        private readonly ManualResetEvent _shutdownRequested = new ManualResetEvent(false);
        private readonly ManualResetEvent _warnAllocation = new ManualResetEvent(false);

        public LowMemoryNotification(long lowMemoryThreshold)
        {
            _logger = LoggingSource.Instance.GetLogger<LowMemoryNotification>("Server");

            _lowMemoryThreshold = lowMemoryThreshold;
            var thread = new Thread(MonitorMemoryUsage)
            {
                IsBackground = true,
                Name = NotificationThreadName
            };
            
            thread.Start();
        }

        public static Size GetCurrentProcessMemoryMappedShared()
        {
            // because we are usually using memory mapped files, we don't want
            // to account for memory that was loaded into our own working set
            // but that the OS can discard with no cost (because it can load
            // the data from disk without needing to write it)

            var stats = MemoryInformation.MemoryStats();

            var sharedMemory = stats.WorkingSet - stats.TotalUnmanagedAllocations - stats.ManagedMemory - stats.MappedTemp;

            // if this is negative, we'll just ignore this
            var mappedShared = new Size(Math.Max(0, sharedMemory), SizeUnit.Bytes);
            return mappedShared;            
        }

        private void MonitorMemoryUsage()
        {
            NativeMemory.EnsureRegistered();

            try
            {
                int clearInactiveHandlersCounter = 0;
                var memoryAvailableHandles = new WaitHandle[] { _simulatedLowMemory, _shutdownRequested };
                var paranoidModeHandles = new WaitHandle[] { _simulatedLowMemory, _shutdownRequested, _warnAllocation };
                var timeout = 5 * 1000;
                while (true)
                {
                    long totalUnmanagedAllocations = 0;

                    var handles = SelectWaitMode(paranoidModeHandles, memoryAvailableHandles);
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

                                totalUnmanagedAllocations += stats.TotalAllocated;
                            }

                            var memInfo = MemoryInformation.GetMemoryInfo();

                            var currentProcessMemoryMappedShared = GetCurrentProcessMemoryMappedShared();
                            var availableMem = (memInfo.AvailableMemory + currentProcessMemoryMappedShared).GetValue(SizeUnit.Bytes);

                            if (availableMem < _lowMemoryThreshold &&
                                // at all times, we want 2% or 1 GB, the lowest of the two
                                memInfo.AvailableMemory < Size.Min((memInfo.TotalPhysicalMemory / 50), new Size(1, SizeUnit.Gigabytes)))
                            {
                                if (LowMemoryState == false)
                                {
                                    if (_logger.IsInfoEnabled)
                                        _logger.Info("Low memory detected, will try to reduce memory usage...");
                                    AddLowMemEvent(LowMemReason.LowMemOnTimeoutChk, availableMem, totalUnmanagedAllocations,
                                        memInfo.TotalPhysicalMemory.GetValue(SizeUnit.Bytes));
                                }
                                LowMemoryState = true;
                                clearInactiveHandlersCounter = 0;
                                RunLowMemoryHandlers(true);
                                timeout = 500;
                            }
                            else
                            {
                                if (LowMemoryState)
                                {
                                    if (_logger.IsInfoEnabled)
                                        _logger.Info("Back to normal memory usage detected");
                                    AddLowMemEvent(LowMemReason.BackToNormal, availableMem, totalUnmanagedAllocations, memInfo.TotalPhysicalMemory.GetValue(SizeUnit.Bytes));
                                }
                                LowMemoryState = false;
                                RunLowMemoryHandlers(false);
                                timeout = availableMem < _lowMemoryThreshold * 2 ? 1000 : 5000;
                            }
                            break;
                        case 0:
                            _simulatedLowMemory.Reset();
                            LowMemoryState = !LowMemoryState;
                            var memInfoForLog = MemoryInformation.GetMemoryInfo();
                            var availableMemForLog = memInfoForLog.AvailableMemory.GetValue(SizeUnit.Bytes);
                            AddLowMemEvent(LowMemoryState ? LowMemReason.LowMemStateSimulation : LowMemReason.BackToNormalSimulation, availableMemForLog,
                                totalUnmanagedAllocations, memInfoForLog.TotalPhysicalMemory.GetValue(SizeUnit.Bytes));
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
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Catastrophic failure in low memory notification: {e.Message}", e);
                }

                throw;
            }
        }

        private WaitHandle[] SelectWaitMode(WaitHandle[] paranoidModeHandles, WaitHandle[] memoryAvailableHandles)
        {
            var memoryInfoResult = MemoryInformation.GetMemoryInfo();
            var handles = (memoryInfoResult.AvailableMemory + GetCurrentProcessMemoryMappedShared()).GetValue(SizeUnit.Bytes) < 
                _lowMemoryThreshold * 2 ? paranoidModeHandles : memoryAvailableHandles;
            return handles;
        }

        private void AddLowMemEvent(LowMemReason reason, long availableMem, long totalUnmanaged, long physicalMem)
        {
            var lowMemEventDetails = new LowMemEventDetails
            {
                Reason = reason,
                FreeMem = availableMem,
                TotalUnmanaged = totalUnmanaged,
                PhysicalMem = physicalMem,
                LowMemThreshold = _lowMemoryThreshold,
                Time = DateTime.UtcNow
            };

            LowMemEventDetailsStack[_lowMemEventDetailsIndex++] = lowMemEventDetails;
            if (_lowMemEventDetailsIndex == LowMemEventDetailsStack.Length)
                _lowMemEventDetailsIndex = 0;
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

