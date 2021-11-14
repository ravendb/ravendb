
using System;
using Raven.Client.Http;

namespace Raven.Client.Extensions
{
    internal static class JavaScriptExtensions
    {
        public static bool UseOptionalChaining(RequestExecutor requestExecutor)     
        {
            var serverVersion = requestExecutor?.LastServerVersion;
            var useOptionalChaining = serverVersion != null ? string.Compare(serverVersion, "5.3", StringComparison.Ordinal) >= 0 : true; // considering by default the latest server version // TODO [shlomo] change to 6.0
            return useOptionalChaining;
        }
    }
}
