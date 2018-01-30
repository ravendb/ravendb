using System;
using System.Collections.Generic;
using System.Threading;
using Sparrow.Collections;
using Sparrow.Logging;
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
            public long CurrentCommitCharge { get; set; }
            public long PhysicalMem { get; set; }
            public long TotalUnmanaged { get; set; }
            public long LowMemThreshold { get; set; }
        }

        public LowMemEventDetails[] LowMemEventDetailsStack = new LowMemEventDetails[256];
        private int _lowMemEventDetailsIndex;
        private int _clearInactiveHandlersCounter;

        private void RunLowMemoryHandlers(bool isLowMemory)
        {
            try
            {
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
                            try
                            {
                                if (_logger.IsInfoEnabled)
                                    _logger.Info("Failure to process low memory notification (low memory handler - " + handler + ")", e);
                            }
                            catch
                            {
                            }
                        }
                    }
                    else
                    {
                        // make sure that we aren't allocating here, we reserve 128 items
                        // and worst case we'll get it in the next run
                        if (_inactiveHandlers.Count < _inactiveHandlers.Capacity)
                            _inactiveHandlers.Add(lowMemoryHandler);
                    }
                }
                foreach (var x in _inactiveHandlers)
                {
                    _lowMemoryHandlers.TryRemove(x);
                }
            }
            finally
            {
                _inactiveHandlers.Clear();
            }
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

        public static readonly LowMemoryNotification Instance = new LowMemoryNotification(
            new Size(1024 * 1024 * 256, SizeUnit.Bytes),
            0.05f);

        public bool LowMemoryState { get; set; }

        public static void Initialize(Size lowMemoryThreshold, float commitChargeThreshold, CancellationToken shutdownNotification)
        {
            Instance._lowMemoryThreshold = lowMemoryThreshold;
            Instance._commitChargeThreshold = commitChargeThreshold;

            shutdownNotification.Register(() => Instance._shutdownRequested.Set());
        }

        private Size _lowMemoryThreshold;
        private float _commitChargeThreshold;
        private readonly ManualResetEvent _simulatedLowMemory = new ManualResetEvent(false);
        private readonly ManualResetEvent _shutdownRequested = new ManualResetEvent(false);
        private readonly ManualResetEvent _warnAllocation = new ManualResetEvent(false);
        private readonly List<WeakReference<ILowMemoryHandler>> _inactiveHandlers = new List<WeakReference<ILowMemoryHandler>>(128);

        public LowMemoryNotification(Size lowMemoryThreshold, float commitChargeThreshold)
        {
            _logger = LoggingSource.Instance.GetLogger<LowMemoryNotification>("Server");

            _lowMemoryThreshold = lowMemoryThreshold;
            _commitChargeThreshold = commitChargeThreshold;

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
            var memoryAvailableHandles = new WaitHandle[] { _simulatedLowMemory, _shutdownRequested };
            var paranoidModeHandles = new WaitHandle[] { _simulatedLowMemory, _shutdownRequested, _warnAllocation };
            var timeout = 5 * 1000;
            try
            {
                while (true)
                {
                    try
                    {
                        var handles = SelectWaitMode(paranoidModeHandles, memoryAvailableHandles);
                        var result = WaitHandle.WaitAny(handles, timeout);
                        switch (result)
                        {
                            case WaitHandle.WaitTimeout:
                                timeout = CheckMemoryStatus();
                                break;
                            case 0:
                                SimulateLowMemory();
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
                    catch (OutOfMemoryException)
                    {
                        try
                        {
                            if (_logger.IsInfoEnabled)
                                _logger.Info("Out of memory error in the low memory notification thread, will wait 5 seconds before trying to check memory status again. The system is likely running out of memory");
                        }
                        catch
                        {
                        }

                        if (_shutdownRequested.WaitOne(5000))
                            return; // shutdown requested
                    }
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                {
                    _logger.Operations("Catastrophic failure in low memory notification", e);
                }
            }
        }

        private void SimulateLowMemory()
        {
            _simulatedLowMemory.Reset();
            LowMemoryState = !LowMemoryState;
            var memInfoForLog = MemoryInformation.GetMemoryInfo();
            var availableMemForLog = memInfoForLog.AvailableMemory.GetValue(SizeUnit.Bytes);
            AddLowMemEvent(LowMemoryState ? LowMemReason.LowMemStateSimulation : LowMemReason.BackToNormalSimulation,
                availableMemForLog,
                -2,
                memInfoForLog.TotalPhysicalMemory.GetValue(SizeUnit.Bytes),
                memInfoForLog.CurrentCommitCharge.GetValue(SizeUnit.Bytes));
            if (_logger.IsInfoEnabled)
                _logger.Info("Simulating : " + (LowMemoryState ? "Low memory event" : "Back to normal memory usage"));
            RunLowMemoryHandlers(LowMemoryState);
        }

        private int CheckMemoryStatus()
        {
            int timeout;
            bool isLowMemory;
            long totalUnmanagedAllocations;
            (Size AvailableMemory, Size TotalPhysicalMemory, Size CurrentCommitCharge) stats;
            try
            {
                isLowMemory = GetLowMemory(out totalUnmanagedAllocations, out stats);
            }
            catch (OutOfMemoryException)
            {
                isLowMemory = true;
                stats = (new Size(), new Size(), new Size());
                totalUnmanagedAllocations = -1;
            }
            if (isLowMemory
            )
            {
                if (LowMemoryState == false)
                {
                    try
                    {
                        if (_logger.IsInfoEnabled)
                        {

                            _logger.Info("Low memory detected, will try to reduce memory usage...");

                        }
                        AddLowMemEvent(LowMemReason.LowMemOnTimeoutChk,
                            stats.AvailableMemory.GetValue(SizeUnit.Bytes),
                            totalUnmanagedAllocations,
                            stats.TotalPhysicalMemory.GetValue(SizeUnit.Bytes),
                            stats.CurrentCommitCharge.GetValue(SizeUnit.Bytes));
                    }
                    catch (OutOfMemoryException)
                    {
                    }
                }
                LowMemoryState = true;
                _clearInactiveHandlersCounter = 0;
                RunLowMemoryHandlers(true);
                timeout = 500;
            }
            else
            {
                if (LowMemoryState)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Back to normal memory usage detected");
                    AddLowMemEvent(LowMemReason.BackToNormal,
                        stats.AvailableMemory.GetValue(SizeUnit.Bytes),
                        totalUnmanagedAllocations,
                        stats.TotalPhysicalMemory.GetValue(SizeUnit.Bytes),
                        stats.CurrentCommitCharge.GetValue(SizeUnit.Bytes));
                }
                LowMemoryState = false;
                RunLowMemoryHandlers(false);
                timeout = stats.AvailableMemory < _lowMemoryThreshold * 2 ? 1000 : 5000;
            }

            return timeout;
        }

        private bool GetLowMemory(out long totalUnmanagedAllocations,
            out (Size AvailableMemory, Size TotalPhysicalMemory, Size CurrentCommitCharge) memStats)
        {
            totalUnmanagedAllocations = 0;
            if (++_clearInactiveHandlersCounter > 60) // 5 minutes == WaitAny 5 Secs * 60
            {
                _clearInactiveHandlersCounter = 0;
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
            var availableMem = (memInfo.AvailableMemory + currentProcessMemoryMappedShared);
            var commitChargePlusMinSizeToKeepFree = memInfo.CurrentCommitCharge + (memInfo.TotalCommittableMemory * _commitChargeThreshold);


            var isLowMemory = availableMem < _lowMemoryThreshold ||
                              // at all times, we want 2% or 1 GB, the lowest of the two
                              memInfo.AvailableMemory < Size.Min((memInfo.TotalPhysicalMemory / 50), new Size(1, SizeUnit.Gigabytes)) ||
                              // we don't have enough room available in the commit charge, going over risking getting OOM from the OS even
                              // if we don't use all that memory
                              commitChargePlusMinSizeToKeepFree > memInfo.TotalCommittableMemory;

            memStats = (availableMem, memInfo.TotalPhysicalMemory, memInfo.TotalCommittableMemory);
            return isLowMemory;
        }

        private WaitHandle[] SelectWaitMode(WaitHandle[] paranoidModeHandles, WaitHandle[] memoryAvailableHandles)
        {
            var memoryInfoResult = MemoryInformation.GetMemoryInfo();
            var memory = (memoryInfoResult.AvailableMemory + GetCurrentProcessMemoryMappedShared());
            var handles = memory < _lowMemoryThreshold * 2 ? paranoidModeHandles : memoryAvailableHandles;
            return handles;
        }

        private void AddLowMemEvent(LowMemReason reason, long availableMem, long totalUnmanaged, long physicalMem, long currentcommitCharge)
        {
            var lowMemEventDetails = new LowMemEventDetails
            {
                Reason = reason,
                FreeMem = availableMem,
                TotalUnmanaged = totalUnmanaged,
                PhysicalMem = physicalMem,
                LowMemThreshold = _lowMemoryThreshold.GetValue(SizeUnit.Bytes),
                CurrentCommitCharge = currentcommitCharge,
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

