using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Utils.Cli
{
    public sealed class RuntimeSettings : ConsoleMessage
    {
        public RuntimeSettings(TextWriter tw) : base(tw)
        {
        }

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
            
            var retaining = IsRetainVMEnabled() ? "retaining" : "not retaining";

            var datas = IsDatasDisabled() ? "disabled" : "enabled";
            
            return $"Using GC in { serverGcMode } {serverGcConcurrentMode} mode {retaining} memory from the OS. DATAS {datas}.";
        }

        private static bool IsRetainVMEnabled()
        {
            var config = GC.GetConfigurationVariables();

            return config.TryGetValue("RetainVM", out var value) && Convert.ToInt32(value) == 1;
        }
        
        private static bool IsDatasDisabled()
        {
            var config = GC.GetConfigurationVariables();

            return config.TryGetValue("GCDynamicAdaptationMode", out var value) && Convert.ToInt32(value) == 0;
        }

        public override void Print()
        {
            var paragraph = new List<ConsoleText>();
            var latencyMode = GCSettings.LatencyMode;

            paragraph.Add(new ConsoleText { Message = "Using GC in ", ForegroundColor = ConsoleColor.Gray });

            if (GCSettings.IsServerGC)
            {
                paragraph.Add(new ConsoleText { Message = "server", ForegroundColor = ConsoleColor.Green });
            }
            else
            {
                paragraph.Add(new ConsoleText { Message = "workstation", ForegroundColor = ConsoleColor.Red });
            }

            if (latencyMode == GCLatencyMode.Batch)
            {
                paragraph.Add(new ConsoleText { Message = " non concurrent", ForegroundColor = ConsoleColor.Red });
            }
            else if (latencyMode == GCLatencyMode.Interactive)
            {
                paragraph.Add(new ConsoleText { Message = " concurrent", ForegroundColor = ConsoleColor.Green });
            }

            paragraph.Add(new ConsoleText { Message = " mode ", ForegroundColor = ConsoleColor.Gray });

            if (IsRetainVMEnabled())
            {
                paragraph.Add(new ConsoleText { Message = "retaining", ForegroundColor = ConsoleColor.Green });
            }
            else
            {
                paragraph.Add(new ConsoleText { Message = "not retaining", ForegroundColor = ConsoleColor.Red });
            }

            paragraph.Add(new ConsoleText { Message = " memory from the OS. ", ForegroundColor = ConsoleColor.Gray });
            
            paragraph.Add(new ConsoleText { Message = $"DATAS {(IsDatasDisabled() ? "disabled" : "enabled")}. ", ForegroundColor = ConsoleColor.Gray, IsNewLinePostPended = true });
            

            ConsoleWriteWithColor(paragraph.ToArray());
        }
    }
}
