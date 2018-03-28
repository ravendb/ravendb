using System;
using Xunit;

namespace FastTests
{
    public class TheoryAndSkipWhen32BitsEnvironment : TheoryAttribute
    {
        public TheoryAndSkipWhen32BitsEnvironment()
        {
            var shouldForceEnvVar = Environment.GetEnvironmentVariable("VORON_INTERNAL_ForceUsing32BitsPager");

            if (bool.TryParse(shouldForceEnvVar, out var result))
                if (result || IntPtr.Size == sizeof(int))
                    Skip = "Not supported for 32 bits";
        }
    }
}
