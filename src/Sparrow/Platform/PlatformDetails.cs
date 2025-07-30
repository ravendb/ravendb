using System;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace Sparrow.Platform
{
    internal static class PlatformDetails
    {
        public static readonly bool IsWindows8OrNewer;

        private static readonly bool IsWindows10OrNewer;

        public static readonly bool Is32Bits = IntPtr.Size == sizeof(int);

#if NET6_0_OR_GREATER
        [SupportedOSPlatformGuard("linux")]
        [SupportedOSPlatformGuard("osx")]
#endif
        public static readonly bool RunningOnPosix = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ||
                                                     RuntimeInformation.IsOSPlatform(OSPlatform.OSX);

#if NET6_0_OR_GREATER
        [SupportedOSPlatformGuard("osx")]
#endif
        public static readonly bool RunningOnMacOsx = RuntimeInformation.IsOSPlatform(OSPlatform.OSX);

#if NET6_0_OR_GREATER
        [SupportedOSPlatformGuard("linux")]
#endif
        public static readonly bool RunningOnLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);

#if NET6_0_OR_GREATER
        [SupportedOSPlatformGuard("windows")]
#endif
        public static readonly bool RunningOnWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

        public static readonly bool RunningOnCortexA53 = RunningOnLinux && CheckIfRunningOnCortexA53();
        public static readonly bool CanPrefetch;
        public static readonly bool CanDiscardMemory;
        internal static readonly bool CanUseHttp2;

        public static bool RunningOnDocker;

        static PlatformDetails()
        {
            RunningOnDocker = string.Equals(Environment.GetEnvironmentVariable("RAVEN_IN_DOCKER"), "true", StringComparison.OrdinalIgnoreCase);

            if (TryGetWindowsVersion(out var version))
            {
                IsWindows8OrNewer = version >= 6.19M;
                IsWindows10OrNewer = version >= 10M;
            }

            CanPrefetch = IsWindows8OrNewer || RunningOnPosix;
            CanDiscardMemory = IsWindows10OrNewer || RunningOnPosix;
            CanUseHttp2 = IsWindows10OrNewer || RunningOnPosix;
        }

        private static bool TryGetWindowsVersion(out decimal version)
        {
            version = -1M;

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) == false)
                return false;

            try
            {
                const string winString = "Windows ";
                var os = RuntimeInformation.OSDescription;

                var idx = os.IndexOf(winString, StringComparison.OrdinalIgnoreCase);
                if (idx < 0)
                    return false;

                var ver = os.Substring(idx + winString.Length);

                // remove second occurence of '.' (win 10 might be 10.123.456)
                var index = ver.IndexOf('.', ver.IndexOf('.') + 1);
                ver = string.Concat(ver.Substring(0, index), ver.Substring(index + 1));

                return decimal.TryParse(ver, NumberStyles.Any, CultureInfo.InvariantCulture, out version);
            }
            catch (DllNotFoundException)
            {
                return false;
            }
        }

        private static bool CheckIfRunningOnCortexA53()
        {
            const string midrPath = "/sys/devices/system/cpu/cpu0/regs/identification/midr_el1";

            if (File.Exists(midrPath) == false)
                return false;

            try
            {
                string text = File.ReadAllText(midrPath).Trim(); // e.g. "410fd034"
                if (ulong.TryParse(text, NumberStyles.HexNumber,
                        CultureInfo.InvariantCulture, out ulong midr))
                {
                    // ARM implementer 0x41 (A), part 0xD03 ⇒ Cortex‑A53
                    return (midr & 0xFFF000u) == 0x41D030u;
                }
            }
            catch (Exception) // file missing, permission, etc.
            {
                // ignore – fall through and report false
            }

            return false;
        }

        internal static string GetVcRedistLink()
        {
            if (RunningOnWindows == false)
                return null;

            if (Is32Bits)
                return "https://aka.ms/vs/17/release/vc_redist.x86.exe";

            return "https://aka.ms/vs/17/release/vc_redist.x64.exe";
        }
    }
}
