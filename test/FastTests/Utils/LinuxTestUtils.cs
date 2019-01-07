using System.IO;
using Voron;
using Voron.Impl.Paging;
using Voron.Platform.Posix;
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

        public static AbstractPager GetNewPager(StorageEnvironmentOptions options, string dataDir, string filename)
        {
            // tests on windows 64bits or linux 64bits only
            if (RunningOnPosix)
            {
                return new RvnMemoryMapPager(options, new VoronPathSetting(Path.Combine(dataDir, filename)));
            }
            return new WindowsMemoryMapPager(options, new VoronPathSetting(Path.Combine(dataDir, filename)));
        }

        public static bool RunningOnPosix => global::Sparrow.Platform.PlatformDetails.RunningOnPosix;
    }
}
