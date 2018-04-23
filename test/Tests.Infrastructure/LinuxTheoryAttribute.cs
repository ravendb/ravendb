using System.Runtime.InteropServices;
using Xunit;

namespace Tests.Infrastructure
{
    public class LinuxTheoryAttribute : TheoryAttribute
    {
        public LinuxTheoryAttribute()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) == false)
            {
                Skip = "Test can be run only on Linux machine";
            }
        }
    }
}
