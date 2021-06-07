using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ThreadsHandler : RequestHandler
    {
        [RavenAction("/admin/debug/threads/stack-trace", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task StackTrace()
        {
            if (PlatformDetails.RunningOnMacOsx)
                throw new NotSupportedException("Capturing live stack traces is not supported by RavenDB on MacOSX");

            var threadIds = GetStringValuesQueryString("threadId", required: false);
            var includeStackObjects = GetBoolValueQueryString("includeStackObjects", required: false) ?? false;

            var sp = Stopwatch.StartNew();
            var threadsUsage = new ThreadsUsage();

            await using (var sw = new StringWriter())
            {
                OutputResultToStream(sw, threadIds.ToHashSet(), includeStackObjects);

                var result = JObject.Parse(sw.GetStringBuilder().ToString());

                var wait = 100 - sp.ElapsedMilliseconds;
                if (wait > 0)
                {
                    // I expect this to be _rare_, but we need to wait to get a correct measure of the cpu
                    await Task.Delay((int)wait);
                }

                var threadStats = threadsUsage.Calculate();
                result["Threads"] = JArray.FromObject(threadStats.List);

                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(result, context));
                    }
                }
            }
        }

        [RavenAction("/admin/debug/threads/runaway", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task RunawayThreads()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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
                }
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void OutputResultToStream(TextWriter sw, HashSet<string> threadIds = null, bool includeStackObjects = false)
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
                {
#pragma warning disable CA1416 // Validate platform compatibility
                    startup.LoadUserProfile = false;
#pragma warning restore CA1416 // Validate platform compatibility
                }

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

                if (PlatformDetails.RunningOnPosix && PlatformDetails.RunningOnMacOsx == false)
                {
                    // enable this process to attach to us
                    Syscall.prctl(Syscall.PR_SET_PTRACER, new UIntPtr((uint)process.Id), UIntPtr.Zero, UIntPtr.Zero, UIntPtr.Zero);

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
                        Syscall.prctl(Syscall.PR_SET_PTRACER, UIntPtr.Zero, UIntPtr.Zero, UIntPtr.Zero, UIntPtr.Zero);
                    }
                }
                if (process.ExitCode != 0)
                    throw new InvalidOperationException("Could not read stack traces, " +
                                                        $"exit code: {process.ExitCode}, error: {sb}");
            }
        }
    }
}
