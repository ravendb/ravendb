using System;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ProcStatsHandler : RequestHandler
    {
        [RavenAction("/admin/debug/cpu/stats", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public Task CpuStats()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var djv = CpuStatsInternal();

                using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(write, djv);
                }
                return Task.CompletedTask;
            }
        }

        [RavenAction("/admin/debug/proc/stats", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public Task ProcStats()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var djv = ProcStatsInternal();

                using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(write, djv);
                }
                return Task.CompletedTask;
            }
        }

        private DynamicJsonValue CpuStatsInternal()
        {
            var proc = Process.GetCurrentProcess();

            var dja = new DynamicJsonArray();
            var djv = new DynamicJsonValue();

            djv["ProcessName"] = proc.ProcessName;
            djv["ProcessorAffinity"] = proc.ProcessorAffinity.ToInt64();
            djv["PrivilegedProcessorTime"] = proc.PrivilegedProcessorTime;
            djv["TotalProcessorTime"] = proc.TotalProcessorTime;
            djv["UserProcessorTime"] = proc.UserProcessorTime;

            dja.Add(djv);

            return new DynamicJsonValue
            {
                ["CpuStats"] = dja
            };
        }

        private DynamicJsonValue ProcStatsInternal()
        {
            var proc = Process.GetCurrentProcess();

            var dja = new DynamicJsonArray();
            var djv = new DynamicJsonValue();
            foreach (var prop in typeof(Process).GetProperties())
            {
                try
                {
                    var value = prop.GetValue(proc);

                    if (value != null)
                    {
                        if ( value.GetType() == typeof(ProcessThreadCollection))
                        {
                            // djv[prop.Name] = GetProcessThreadCollection((ProcessThreadCollection)value);
                            djv[prop.Name] = GetProcessCollection<ProcessThread>((ProcessThreadCollection)value);
                        }
                        else if (value.GetType() == typeof(ProcessModuleCollection))
                        {
                            // djv[prop.Name] = GetProcessThreadCollection((ProcessModuleCollection)value);
                            djv[prop.Name] = GetProcessCollection<ProcessModule>((ProcessModuleCollection)value);
                        }
                        else
                        {
                            AddGenericValue(djv, prop, value);
                        }
                    }
                    else
                    {
                        djv[prop.Name] = "null";
                    }
                }
                catch (Exception ex)
                {
                    if (ex.InnerException != null)
                    {
                        if (ex.InnerException.Message.Contains("Process must exit before requested information can be determined") == false)
                            djv[prop.Name] = "Not Available : " + ex.InnerException.Message;
                    }
                    else
                        djv[prop.Name] = "Not Available : " + ex.Message;
                }
            }
            
            dja.Add(djv);

            return new DynamicJsonValue
            {
                ["ProcStats"] = dja
            };
        }

        [SuppressMessage("ReSharper", "OperatorIsCanBeUsed")]
        private static void AddGenericValue(DynamicJsonValue djv, MemberInfo prop, object value)
        {
            if (value.GetType() == typeof(IntPtr))
            {
                djv[prop.Name] = (long)(IntPtr)value;
            }
            else if (value.GetType() == typeof(SafeProcessHandle))
            {
                djv[prop.Name] = (long)((SafeProcessHandle)value).DangerousGetHandle();
            }
            else
            {
                switch (Type.GetTypeCode(value.GetType()))
                {
                    case TypeCode.Byte:
                    case TypeCode.SByte:
                    case TypeCode.UInt16:
                    case TypeCode.UInt32:
                    case TypeCode.UInt64:
                    case TypeCode.Int16:
                    case TypeCode.Int32:
                    case TypeCode.Int64:
                    case TypeCode.Decimal:
                    case TypeCode.Double:
                    case TypeCode.Single:
                    case TypeCode.DateTime:
                    case TypeCode.Boolean:
                    case TypeCode.String:
                        djv[prop.Name] = value;
                        break;
                    default:
                        djv[prop.Name] = value.ToString();
                        break;
                }
            }
        }

        private DynamicJsonArray GetProcessCollection<T>(ReadOnlyCollectionBase threadCollection)
        {
            var dja = new DynamicJsonArray();
            foreach (T procThread in threadCollection)
            {
                var djv = new DynamicJsonValue();
                foreach (var prop in typeof(T).GetProperties())
                {
                    try
                    {
                        var value = prop.GetValue(procThread);

                        if (value != null)
                        {
                            AddGenericValue(djv, prop, value);
                        }
                        else
                        {
                            djv[prop.Name] = "null";
                        }
                    }
                    catch
                    {
                        // ignore - too much information
                    }
                }

                dja.Add(djv);
            }

            return dja;
        }
    }
}
