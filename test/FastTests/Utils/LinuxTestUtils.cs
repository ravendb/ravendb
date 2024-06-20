using System.IO;
using Voron;
using Voron.Impl.Paging;
using Voron.Platform.Win32;
using Voron.Util.Settings;

namespace FastTests.Utils
{
    public static class LinuxTestUtils
    {
        public static string Dos2Unix(string str)
        {
            if (RunningOnPosix)
                return str.Replace("\r\n", "\n");
            return str;
        }

        public static bool RunningOnPosix => global::Sparrow.Platform.PlatformDetails.RunningOnPosix;
    }
}
