using System.Runtime.InteropServices;
using System;


namespace Sparrow.Platform
{
    public static class Platform
    {
        public static readonly bool RunningOnPosix = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ||
                                                     RuntimeInformation.IsOSPlatform(OSPlatform.OSX);

        public static readonly bool CanPrefetch = IsWindows8OrNewer();

        private static bool IsWindows8OrNewer()
        {
            var winString = "Windows ";
            var os = RuntimeInformation.OSDescription;

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) == false)
                return false;

            var idx = os.IndexOf(winString, StringComparison.OrdinalIgnoreCase);
            if (idx < 0)
                return false;

            var ver = os.Substring(idx + winString.Length);

            if (ver != null)
            {
                // remove second occurance of '.' (win 10 might be 10.123.456)
                var index = ver.IndexOf('.', ver.IndexOf('.') + 1);
                ver = string.Concat(ver.Substring(0, index), ver.Substring(index + 1));

                decimal output;
                if (decimal.TryParse(ver, out output) == true)
                {
                    return output >= 6.19M; // 6.2 is win8, 6.1 win7..
                }
            }

            return false;


        }
    }
}