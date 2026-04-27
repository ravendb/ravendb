using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Global;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;
using Sparrow.Server.Platform.Posix.macOS;

namespace Sparrow.Server.Platform
{
    public static class CpuTopology
    {
        // Conservative default for boxes where probing fails or returns nothing useful
        // (older kernels, ARM64 Linux without sysfs cache nodes, Apple Silicon's unified
        // memory hierarchy that does not expose a classical L3, Windows on ARM64, etc.).
        private const long DefaultL3CacheSize = 8L * Constants.Size.Megabyte;

        private static readonly Lazy<long> L3CacheSizeLazy = new(ProbeL3CacheSize, LazyThreadSafetyMode.PublicationOnly);

        public static long L3CacheSize => L3CacheSizeLazy.Value;

        private static long ProbeL3CacheSize()
        {
            try
            {
                if (PlatformDetails.RunningOnLinux)
                    return ProbeLinux();
                if (PlatformDetails.RunningOnMacOsx)
                    return ProbeMacOs();
                if (PlatformDetails.RunningOnWindows)
                    return ProbeWindows();
            }
            catch
            {
                // best-effort: fall through to the default
            }

            return DefaultL3CacheSize;
        }

        private static long ProbeLinux()
        {
            // Prefer sysconf: it works without /sys mounted (containers with restricted
            // filesystem views) and on x86_64 glibc returns the host L3 in bytes directly.
            // sysconf returns 0 on ARM64 kernels and on musl, so fall back to sysfs there.
            long fromSysconf = Syscall.sysconf(PerPlatformValues.SysconfNames._SC_LEVEL3_CACHE_SIZE);
            if (fromSysconf > 0)
                return fromSysconf;

            const string path = "/sys/devices/system/cpu/cpu0/cache/index3/size";
            if (File.Exists(path) == false)
                return DefaultL3CacheSize;

            return ParseSizeWithUnit(File.ReadAllText(path));
        }

        private static long ParseSizeWithUnit(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw))
                return DefaultL3CacheSize;

            var s = raw.Trim();
            long multiplier = 1;
            var suffix = s[^1];
            if (suffix is 'K' or 'k') { multiplier = Constants.Size.Kilobyte; s = s[..^1]; }
            else if (suffix is 'M' or 'm') { multiplier = Constants.Size.Megabyte; s = s[..^1]; }
            else if (suffix is 'G' or 'g') { multiplier = Constants.Size.Gigabyte; s = s[..^1]; }

            return long.TryParse(s, out var value) && value > 0
                ? value * multiplier
                : DefaultL3CacheSize;
        }

        private static unsafe long ProbeMacOs()
        {
            long value = 0;
            int len = sizeof(long);
            int rc = macSyscall.sysctlbyname("hw.l3cachesize", &value, &len, null, UIntPtr.Zero);
            if (rc == 0 && value > 0)
                return value;

            // Apple Silicon reports 0 here: the SoC exposes a system-level cache instead
            // of a per-die L3, and its size varies by chip and is not surfaced via sysctl.
            // The 8 MiB default is conservative for every shipping M-series part.
            return DefaultL3CacheSize;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct CacheDescriptor
        {
            public byte Level;
            public byte Associativity;
            public ushort LineSize;
            public uint Size;
            public int Type;
        }

        [StructLayout(LayoutKind.Explicit)]
        private struct ProcessorInfoUnion
        {
            [FieldOffset(0)] public CacheDescriptor Cache;
            [FieldOffset(0)] public ulong Reserved0;
            [FieldOffset(8)] public ulong Reserved1;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct SystemLogicalProcessorInformation
        {
            public nuint ProcessorMask;
            public int Relationship;
            public ProcessorInfoUnion Info;
        }

        private const int RelationCache = 2;

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool GetLogicalProcessorInformation(IntPtr buffer, out uint returnedLength);

        private static unsafe long ProbeWindows()
        {
            // First call probes the required buffer size; success is signalled by setting
            // length even though the call returns false with ERROR_INSUFFICIENT_BUFFER.
            // If length is still zero the API is unavailable (e.g. ARM64 host); bail out.
            GetLogicalProcessorInformation(IntPtr.Zero, out uint length);
            if (length == 0)
                return DefaultL3CacheSize;

            var buffer = Marshal.AllocHGlobal((int)length);
            try
            {
                if (GetLogicalProcessorInformation(buffer, out length) == false)
                    return DefaultL3CacheSize;

                int entrySize = sizeof(SystemLogicalProcessorInformation);
                int count = (int)length / entrySize;
                // Take the smallest L3 reported. On asymmetric topologies (multi-CCD AMD,
                // future hybrid Windows-on-ARM split-LLC, etc.) a task may land on the
                // smaller cache; sizing for that cache is conservative and safe. Symmetric
                // hosts report the same size in every entry, so min == max in the common case.
                long smallest = long.MaxValue;
                var ptr = (SystemLogicalProcessorInformation*)buffer;
                for (int i = 0; i < count; i++)
                {
                    ref var entry = ref ptr[i];
                    if (entry.Relationship != RelationCache)
                        continue;
                    if (entry.Info.Cache.Level != 3)
                        continue;
                    if (entry.Info.Cache.Size > 0 && entry.Info.Cache.Size < smallest)
                        smallest = entry.Info.Cache.Size;
                }

                return smallest != long.MaxValue ? smallest : DefaultL3CacheSize;
            }
            finally
            {
                Marshal.FreeHGlobal(buffer);
            }
        }
    }
}
