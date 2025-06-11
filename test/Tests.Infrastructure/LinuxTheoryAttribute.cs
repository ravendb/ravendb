﻿using System.Runtime.InteropServices;
using Xunit.Sdk;

namespace Tests.Infrastructure
{
    public class LinuxTheoryAttribute : RavenTheoryAttribute
    {
        public LinuxTheoryAttribute(RavenTestCategory category = RavenTestCategory.Linux) : base(category)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) == false)
            {
                Skip = "Test can be run only on Linux machine";
            }
        }
    }
}
