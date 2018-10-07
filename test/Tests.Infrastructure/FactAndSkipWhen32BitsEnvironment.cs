using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace FastTests
{
    class FactAndSkipWhen32BitsEnvironment : FactAttribute
    {
        public FactAndSkipWhen32BitsEnvironment()
        {
            var shouldForceEnvVar = Environment.GetEnvironmentVariable("VORON_INTERNAL_ForceUsing32BitsPager");

            if (bool.TryParse(shouldForceEnvVar, out var result))
                if (result)
                    Skip = "Not supported for 32 bits";
            if(IntPtr.Size == sizeof(int))
                Skip = "Not supported for 32 bits";
        }
    }
}
