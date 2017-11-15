using System.Runtime.InteropServices;
using Xunit;

namespace FastTests
{
    public class LinuxFactAttribute : FactAttribute
    {
        public LinuxFactAttribute()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) == false)
            {
                Skip = "Test can be run only on Linux machine";
            }
        }
    }
}
