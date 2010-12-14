using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Raven.Tests
{
    public class WindowsExclusiveFactAttribute : FactAttribute
    {
        public WindowsExclusiveFactAttribute()
        {
            if (IsNotSupportedSystem())
            {
                Skip = "Ignored on Unix/Mac/etc";
            }
        }
 
        public static bool IsNotSupportedSystem()
        {
            return Environment.OSVersion.Platform == PlatformID.Unix || Environment.OSVersion.Platform == PlatformID.MacOSX;
        }
    }
}
