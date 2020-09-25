using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime;
using Sparrow.Json;

namespace Raven.Server.Utils.Cli
{
    public class RuntimeSettings : ConsoleMessage
    {
        public RuntimeSettings(TextWriter tw) : base(tw)
        {
        }

        private static (bool HasValue, bool Value) TryGetRetainVMSettingValue()
        {
            // REMARK: Whenever CoreCLR includes this we can get rid of this method.
            var runtimeConfigurationPath = Path.Combine(AppContext.BaseDirectory, "Raven.Server.runtimeconfig.json");

            // Try read runtime configuration from file
            if (File.Exists(runtimeConfigurationPath))
            {
                using (var context = JsonOperationContext.ShortTermSingleUse())
                using (FileStream f = File.OpenRead(runtimeConfigurationPath))
                using (BlittableJsonReaderObject @object = context.Read(f, "n"))
                {
                    if (@object.TryGet("runtimeOptions", out BlittableJsonReaderObject runtime) &&
                        runtime.TryGet("configProperties", out BlittableJsonReaderObject properties) &&
                        properties.TryGet("System.GC.RetainVM", out bool value))
                    {
                        return (true, value);
                    }
                }
            }

            // Fallback (Environment Variable)
            string retainVM = Environment.GetEnvironmentVariable("GCRetainVM");
            if (!string.IsNullOrEmpty(retainVM))
            {
                if (Boolean.TryParse(retainVM, out bool value))
                {
                    return (true, value);
                }
            }

            return (false, false);
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

            // https://github.com/dotnet/runtime/issues/42720
            /*
            var retaining = "not retaining";
            (bool HasValue, bool Value) retainVmSettingValue = TryGetRetainVMSettingValue();
            if (retainVmSettingValue.HasValue && retainVmSettingValue.Value)
            {
                retaining = "retaining";
            }

            return $"Using GC in { serverGcMode } {serverGcConcurrentMode} mode {retaining} memory from the OS.";
            */
            return $"Using GC in { serverGcMode } {serverGcConcurrentMode} mode.";
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

            paragraph.Add(new ConsoleText { Message = " mode.", ForegroundColor = ConsoleColor.Gray, IsNewLinePostPended = true });

            // https://github.com/dotnet/runtime/issues/42720
            /*
            paragraph.Add(new ConsoleText { Message = " mode ", ForegroundColor = ConsoleColor.Gray });

            (bool HasValue, bool Value) retainVmSettingValue = TryGetRetainVMSettingValue();
            if (retainVmSettingValue.HasValue && retainVmSettingValue.Value)
            {
                paragraph.Add(new ConsoleText { Message = "retaining", ForegroundColor = ConsoleColor.Green });
            }
            else
            {
                paragraph.Add(new ConsoleText { Message = "not retaining", ForegroundColor = ConsoleColor.Red });
            }

            paragraph.Add(new ConsoleText { Message = " memory from the OS.", ForegroundColor = ConsoleColor.Gray, IsNewLinePostPended = true });
            */

            ConsoleWriteWithColor(paragraph.ToArray());
        }
    }
}
