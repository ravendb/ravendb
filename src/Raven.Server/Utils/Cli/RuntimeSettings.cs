namespace Raven.Server.Utils.Cli
{
    public static class RuntimeSettings
    {
        public static string Describe()
        {
            var serverGcMode = System.Runtime.GCSettings.IsServerGC
                ? "server" : "workstation";
            
            // return $"Using GC in { serverGcMode } {concurrent} mode {not) retaining | retaining} memory from the OS. ")
            return $"Using GC in {serverGcMode} mode.";
        }
    }
}
