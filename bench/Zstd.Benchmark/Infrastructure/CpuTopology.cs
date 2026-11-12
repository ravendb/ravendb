using System;
using System.Runtime.InteropServices;

namespace Zstd.Benchmark.Infrastructure
{
    /// <summary>
    /// On hybrid CPUs (P-cores + E-cores) benchmark processes must be pinned to one core type, otherwise the scheduler
    /// moving the process between core types dominates the variance.
    /// </summary>
    internal static class CpuTopology
    {
        /// <summary>
        /// Affinity mask of the logical processors in the most performant efficiency class, or null when the CPU is not hybrid
        /// (or the information is not available).
        /// </summary>
        public static IntPtr? GetPerformanceCoresMask(out string description)
        {
            description = "not a hybrid CPU, no affinity";
            if (OperatingSystem.IsWindows() == false || IntPtr.Size != 8)
                return null;

            GetSystemCpuSetInformation(IntPtr.Zero, 0, out uint length, IntPtr.Zero, 0);
            if (length == 0)
                return null;

            IntPtr buffer = Marshal.AllocHGlobal((int)length);
            try
            {
                if (GetSystemCpuSetInformation(buffer, length, out length, IntPtr.Zero, 0) == false)
                    return null;

                // SYSTEM_CPU_SET_INFORMATION: Size(4) Type(4) Id(4) Group(2) LogicalProcessorIndex(1) CoreIndex(1)
                //                             LastLevelCacheIndex(1) NumaNodeIndex(1) EfficiencyClass(1) ...
                byte maxClass = 0, minClass = byte.MaxValue;
                for (int offset = 0; offset < length;)
                {
                    int size = Marshal.ReadInt32(buffer, offset);
                    if (Marshal.ReadInt16(buffer, offset + 12) == 0)
                    {
                        byte efficiencyClass = Marshal.ReadByte(buffer, offset + 18);
                        maxClass = Math.Max(maxClass, efficiencyClass);
                        minClass = Math.Min(minClass, efficiencyClass);
                    }
                    offset += size;
                }

                if (maxClass == minClass)
                    return null;

                ulong mask = 0;
                int count = 0;
                for (int offset = 0; offset < length;)
                {
                    int size = Marshal.ReadInt32(buffer, offset);
                    if (Marshal.ReadInt16(buffer, offset + 12) == 0 && Marshal.ReadByte(buffer, offset + 18) == maxClass)
                    {
                        mask |= 1UL << Marshal.ReadByte(buffer, offset + 14);
                        count++;
                    }
                    offset += size;
                }

                description = $"hybrid CPU, pinned to {count} performance-core logical processors (mask 0x{mask:X})";
                return (IntPtr)(long)mask;
            }
            finally
            {
                Marshal.FreeHGlobal(buffer);
            }
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool GetSystemCpuSetInformation(IntPtr information, uint bufferLength, out uint returnedLength, IntPtr process, uint flags);
    }
}
