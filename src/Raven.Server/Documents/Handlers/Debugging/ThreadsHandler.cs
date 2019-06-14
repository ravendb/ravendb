using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
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
            var threadIds = GetStringValuesQueryString("threadId", required: false);
            var includeStackObjects = GetBoolValueQueryString("includeStackObjects", required: false) ?? false;

            using (var stream = new MemoryStream())
            {
                using (var sw = new StreamWriter(stream))
                {
                    OutputResultToStream(sw, threadIds.ToHashSet(), includeStackObjects);
                 
                    sw.Flush();
                    
                    stream.Position = 0;
                    stream.WriteTo(ResponseBodyStream());    
                }
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
        public static void OutputResultToStream(StreamWriter sw, HashSet<string> threadIds = null, bool includeStackObjects = false)
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

                var process = new Process
                {
                    StartInfo = startup,
                    EnableRaisingEvents = true
                };

                sb.Clear();
                process.OutputDataReceived += (sender, args) => sw.Write(args.Data);
                process.ErrorDataReceived += (sender, args) => sb.Append(args.Data);

                process.Start();

                process.BeginErrorReadLine();
                process.BeginOutputReadLine();

                if(PlatformDetails.RunningOnPosix && PlatformDetails.RunningOnMacOsx == false)
                {
                    // enable this process to attach to us
                    prctl(PR_SET_PTRACER, new UIntPtr((uint)process.Id), UIntPtr.Zero, UIntPtr.Zero, UIntPtr.Zero);

                    process.StandardInput.WriteLine("go");// value is meaningless, just need a new line
                    process.StandardInput.Flush();
                }

                try
                {
                    process.WaitForExit();
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
            }
        }

        [DllImport("libc", SetLastError = true)]
        private static extern int prctl(int option, UIntPtr arg2, UIntPtr arg3, UIntPtr arg4, UIntPtr arg5);
        private const int PR_SET_PTRACER = 0x59616d61;
    }
}
