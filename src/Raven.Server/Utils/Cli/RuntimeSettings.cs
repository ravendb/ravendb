using System.Runtime;

namespace Raven.Server.Utils.Cli
{
    public static class RuntimeSettings
    {
        public static string Describe()
        {
            var latencyMode = GCSettings.LatencyMode;
            var isServerMode = GCSettings.IsServerGC;

            var serverGcMode = isServerMode
                ? "server" : "workstation";

            var serverGcConcurrentMode = "";
            if (latencyMode == GCLatencyMode.Batch)
            {
                serverGcConcurrentMode = "non concurrent";
            }
            else if (latencyMode == GCLatencyMode.Interactive)
            {
                serverGcConcurrentMode = "concurrent";
            }

            var retaining = "not retaining";
            if (latencyMode == GCLatencyMode.LowLatency || latencyMode == GCLatencyMode.SustainedLowLatency)
            {
                retaining = "retaining";
            }

            return $"Using GC in { serverGcMode } {serverGcConcurrentMode} mode {retaining} memory from the OS.";
        }
    }
}
