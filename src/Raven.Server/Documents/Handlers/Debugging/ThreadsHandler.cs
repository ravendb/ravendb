using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Platform;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ThreadsHandler : RequestHandler
    {
        [RavenAction("/admin/debug/threads/stack-trace", "GET", AuthorizationStatus.Operator)]
        public Task StackTrace()
        {
            if (PlatformDetails.RunningOnMacOsx)
                throw new NotSupportedException("Capturing live stack traces is not supported by RavenDB on MacOSX");

            var threadIds = GetStringValuesQueryString("threadId", required: false);
            var includeStackObjects = GetBoolValueQueryString("includeStackObjects", required: false) ?? false;

            var sp = Stopwatch.StartNew();
            var threadsUsage = new ThreadsUsage();

            var r = OutputResultToStream(threadIds.ToHashSet(), includeStackObjects);

            var result = JObject.Parse(r);

            var wait = 100 - sp.ElapsedMilliseconds;
            if (wait > 0)
            {
                // I expect this to be _rare_, but we need to wait to get a correct measure of the cpu
                Thread.Sleep((int)wait);
            }

            var threadStats = threadsUsage.Calculate();
            result["Threads"] = JArray.FromObject(threadStats.List);

            using (var writer = new StreamWriter(ResponseBodyStream()))
            {
                result.WriteTo(new JsonTextWriter(writer) {Indentation = 4});
                writer.Flush();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/admin/debug/threads/runaway", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task RunawayThreads()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    try
                    {
                        var threadsUsage = new ThreadsUsage();

                        // need to wait to get a correct measure of the cpu
                        await Task.Delay(100);

                        var result = threadsUsage.Calculate();
                        context.Write(writer,
                            new DynamicJsonValue
                            {
                                ["Runaway Threads"] = result.ToJson()
                            });
                    }
                    catch (Exception e)
                    {
                        context.Write(writer,
                            new DynamicJsonValue
                            {
                                ["Error"] = e.ToString()
                            });
                    }

                    writer.Flush();
                }
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static unsafe string OutputResultToStream(HashSet<string> threadIds = null, bool includeStackObjects = false)
        {
            var ravenDebugExec = Path.Combine(AppContext.BaseDirectory,
                PlatformDetails.RunningOnPosix ? "Raven.Debug" : "Raven.Debug.exe"
            );

            if (File.Exists(ravenDebugExec) == false)
                throw new FileNotFoundException($"Could not find debugger tool at '{ravenDebugExec}'");

            using (var currentProcess = Process.GetCurrentProcess())
            {
                var sb = new StringBuilder($"stack-traces --pid {currentProcess.Id}");

                if (PlatformDetails.RunningOnPosix && PlatformDetails.RunningOnMacOsx == false)
                    sb.Append(" --wait");

                if (threadIds != null && threadIds.Count > 0)
                {
                    foreach (var threadId in threadIds)
                    {
                        if (int.TryParse(threadId, out _) == false)
                            throw new ArgumentException($"Could not parse thread id with value '{threadId}' to number.");

                        sb.Append($" --tid {threadId}");
                    }
                }

                if (includeStackObjects)
                    sb.Append(" --includeStackObjects");

                var startup = new ProcessStartInfo
                {
                    Arguments = sb.ToString(),
                    FileName = ravenDebugExec,
                    WindowStyle = ProcessWindowStyle.Normal,
                    RedirectStandardError = true,
                    RedirectStandardOutput = true,
                    RedirectStandardInput = true,
                    UseShellExecute = false
                };
                if (PlatformDetails.RunningOnPosix == false)
                    startup.LoadUserProfile = false;

                var process = RavenProcess.Start(startup);

                sb.Clear();

                if (PlatformDetails.RunningOnPosix && PlatformDetails.RunningOnMacOsx == false)
                {
                    // enable this process to attach to us
                    prctl(PR_SET_PTRACER, (UIntPtr)process.Pid.ToPointer(), UIntPtr.Zero, UIntPtr.Zero, UIntPtr.Zero);

                    process.StandardInput.WriteLine("go");// value is meaningless, just need a new line
                }

                var read = process.StandardOutput.ReadToEndAsync();

                try
                {
                    //TODO: here we're leaving a process hanging and we need to account for server shutdown
                    while (process.WaitForExit() == false);
                }
                finally
                {
                    if (PlatformDetails.RunningOnPosix && PlatformDetails.RunningOnMacOsx == false)
                    {
                        // disable attachments 
                        prctl(PR_SET_PTRACER, UIntPtr.Zero, UIntPtr.Zero, UIntPtr.Zero, UIntPtr.Zero);
                    }
                }
                if (process.ExitCode != 0)
                    throw new InvalidOperationException("Could not read stack traces, " +
                                                        $"exit code: {process.ExitCode}, error: {sb}");

                return read.Result;
            }
        }

        [DllImport("libc", SetLastError = true)]
        private static extern int prctl(int option, UIntPtr arg2, UIntPtr arg3, UIntPtr arg4, UIntPtr arg5);
        private const int PR_SET_PTRACER = 0x59616d61;
    }
}
