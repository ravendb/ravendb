using System.Text.RegularExpressions;

namespace Raven.Server.Utils
{
    internal static class WindowsServiceUtils
    {
        // Shared between the rvn registrar (tools/rvn/WindowsService.cs, linked into rvn via
        // <Compile Include>) and the Raven.Server host (WindowsServiceRunner) so both normalize
        // the service name identically: rvn registers the service under this name and the host
        // reports the same name to the SCM.
        public static string NormalizeServiceName(string serviceName)
        {
            return Regex.Replace(serviceName, @"[\/\s]", "_");
        }
    }
}
