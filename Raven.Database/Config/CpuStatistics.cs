// -----------------------------------------------------------------------
//  <copyright file="CpuStatistics.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

using System.Web.UI.WebControls;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;

using Raven.Abstractions.Logging;

using ConfigurationManager = System.Configuration.ConfigurationManager;
using FILETIME = System.Runtime.InteropServices.ComTypes.FILETIME;
using Sparrow.Collections;

namespace Raven.Database.Config
{
    public static class CpuStatistics
    {
        private const float HighNotificationThreshold = 0.8f;
        private const float LowNotificationThreshold = 0.6f;

        private const int NumberOfItemsInQueue = 5;

        private static readonly float[] LastUsages = new float[NumberOfItemsInQueue];

        private static readonly ConcurrentSet<WeakReference<ICpuUsageHandler>> CpuUsageHandlers = new ConcurrentSet<WeakReference<ICpuUsageHandler>>();

        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private static int nextWriteIndex;
        private static readonly ManualResetEventSlim _domainUnload = new ManualResetEventSlim();
        private static bool dynamicLoadBalancing;

        public static double Average { get; private set; }
        public static readonly FixedSizeConcurrentQueue<CpuUsageCallsRecord> CpuUsageCallsRecordsQueue = new FixedSizeConcurrentQueue<CpuUsageCallsRecord>(100);

        static CpuStatistics()
        {
            if (bool.TryParse(ConfigurationManager.AppSettings["Raven/DynamicLoadBalancing"], out dynamicLoadBalancing) &&
                dynamicLoadBalancing == false)
                return; // disabled, so we avoid it
            dynamicLoadBalancing = true;

            AppDomain.CurrentDomain.DomainUnload += (sender, args) => _domainUnload.Set();

            new Thread(() =>
            {
                try
                {
                    var usage = new CpuUsage();
                    while (true)
                    {
                        if (_domainUnload.Wait(1000))
                            return;

                        var totalUsage = usage.GetCurrentUsage();
                        try
                        {
                            HandleCpuUsage(totalUsage);
                        }
                        catch (Exception e)
                        {
                            Log.WarnException("Failed to notify handlers about CPU usage, aborting CPU throttling", e);
                            return;
                        }
                    }
                }
                catch (Exception e)
                {
                    Log.ErrorException("Errpr handling CPU statistics during automatic CPU throttling, aborting automatic thorttling!", e);
                }
            })
            {
                IsBackground = true,
                Name = "CPU usage notification thread"
            }.Start();
        }

        public static void HandleCpuUsage(float usageInPercents)
        {

            var stats = new CpuUsageCallsRecord
            {
                StartedAt = SystemTime.UtcNow,
            };

            var previousWriteIndex = nextWriteIndex;
            LastUsages[previousWriteIndex] = usageInPercents;
            nextWriteIndex = (nextWriteIndex + 1) % NumberOfItemsInQueue;

            if (previousWriteIndex < NumberOfItemsInQueue - 1) // waiting for queue to fill up
                return;

            nextWriteIndex = 0;

            var average = Average = LastUsages.Average();
            if (average < 0)
                return; // there was an error in getting the CPU stats, ignoring

            CpuUsageCallsRecord cpuUsage;
            CpuUsageCallsRecordsQueue.TryPeek(out cpuUsage);

            if (average >= HighNotificationThreshold)
            {
                if (cpuUsage?.Reason != CpuUsageLevel.HighCpuUsage)
                {
                    stats.Reason = CpuUsageLevel.HighCpuUsage;
                    CpuUsageCallsRecordsQueue.Enqueue(stats);
                }
                RunCpuUsageHandlers(handler => handler.HandleHighCpuUsage());
            }
            else if (average < LowNotificationThreshold)
            {
                if (cpuUsage?.Reason != CpuUsageLevel.LowCpuUsage)
                {
                    stats.Reason = CpuUsageLevel.LowCpuUsage;
                    CpuUsageCallsRecordsQueue.Enqueue(stats);
                }
                RunCpuUsageHandlers(handler => handler.HandleLowCpuUsage());
            }
            //Normal CPU usage
            else if (cpuUsage?.Reason != CpuUsageLevel.NormalCpuUsage)
            {

                stats.Reason = CpuUsageLevel.NormalCpuUsage;
                CpuUsageCallsRecordsQueue.Enqueue(stats);

            }

        }

        public static void RegisterCpuUsageHandler(ICpuUsageHandler handler)
        {
            if (dynamicLoadBalancing == false)
                return;
            CpuUsageHandlers.Add(new WeakReference<ICpuUsageHandler>(handler));
        }

        private static void RunCpuUsageHandlers(Action<ICpuUsageHandler> action)
        {
            var inactiveHandlers = new List<WeakReference<ICpuUsageHandler>>();

            foreach (var highCpuUsageHandler in CpuUsageHandlers)
            {
                ICpuUsageHandler handler;
                if (highCpuUsageHandler.TryGetTarget(out handler))
                {
                    try
                    {
                        action(handler);
                    }
                    catch (Exception e)
                    {
                        Log.Error("Failure to process CPU usage notification (cpu usage handler - " + handler + "), handler will be removed", e);
                        inactiveHandlers.Add(highCpuUsageHandler);
                    }
                }
                else
                {
                    inactiveHandlers.Add(highCpuUsageHandler);
                }
            }

            inactiveHandlers.ForEach(x => CpuUsageHandlers.TryRemove(x));
        }
    }

    public interface ICpuUsageHandler
    {
        void HandleHighCpuUsage();

        void HandleLowCpuUsage();
    }

    public class CpuUsage
    {
        private ulong _previousIdleTicks;
        private ulong _previousTotalTicks;
        private float _previousResult;

        public CpuUsage()
        {
            FILETIME currentIdleTime;
            FILETIME currentKernelTime;
            FILETIME currentUserTime;
            if (GetSystemTimes(out currentIdleTime, out currentKernelTime, out currentUserTime) == false)
                throw new Win32Exception();

            _previousIdleTicks = FileTimeToULong(currentIdleTime);

            _previousTotalTicks = FileTimeToULong(currentKernelTime) + FileTimeToULong(currentUserTime);
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool GetSystemTimes(
            out FILETIME lpIdleTime,
            out FILETIME lpKernelTime,
            out FILETIME lpUserTime);

        public float GetCurrentUsage()
        {
            FILETIME currentIdleTime;
            FILETIME currentKernelTime;
            FILETIME currentUserTime;
            if (GetSystemTimes(out currentIdleTime, out currentKernelTime, out currentUserTime) == false)
                throw new Win32Exception();

            ulong idleTicks = FileTimeToULong(currentIdleTime);
            ulong totalTicks = FileTimeToULong(currentKernelTime) + FileTimeToULong(currentUserTime);
            ulong totalTicksSinceLastTime = totalTicks - _previousTotalTicks;
            ulong idleTicksSinceLastTime = idleTicks - _previousIdleTicks;

            float ret;
            if (totalTicksSinceLastTime == 0)
            {
                ret = _previousResult;
            }
            else
            {
                ret = 1.0f - ((float)idleTicksSinceLastTime) / totalTicksSinceLastTime;
            }
            _previousResult = ret;
            _previousTotalTicks = totalTicks;
            _previousIdleTicks = idleTicks;
            return ret;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ulong FileTimeToULong(FILETIME time)
        {
            return ((ulong)time.dwHighDateTime << 32) + (uint)time.dwLowDateTime;
        }
    }
}
