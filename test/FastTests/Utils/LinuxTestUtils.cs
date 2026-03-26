using System.IO;
using Voron;
using Voron.Impl.Paging;
using Voron.Platform.Win32;
using Voron.Util.Settings;

namespace FastTests.Utils
{
    public static class LinuxTestUtils
    {
        public static bool RunningOnPosix => global::Sparrow.Platform.PlatformDetails.RunningOnPosix;
    }
}
