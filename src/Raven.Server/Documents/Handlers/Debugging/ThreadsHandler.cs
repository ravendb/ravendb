using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ThreadsHandler : RequestHandler
    {
        [RavenAction("/admin/debug/threads/stack-trace", "GET", AuthorizationStatus.Operator)]
        public Task StackTrace()
        {
            if (Debugger.IsAttached)
                throw new InvalidOperationException("Cannot get stack traces when debugger is attached");

            var threadIds = GetStringValuesQueryString("threadId", required: false);
            var includeStackObjects = GetBoolValueQueryString("includeStackObjects", required: false) ?? false;

            using (var sw = new StreamWriter(ResponseBodyStream()))
            {
                OutputResultToStream(sw, threadIds.ToHashSet(), includeStackObjects);
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

        public static void OutputResultToStream(StreamWriter sw, HashSet<string> threadIds = null, bool includeStackObjects = false)
        {
            var ravenDebugExec = Path.Combine(AppContext.BaseDirectory, "Raven.Debug.exe");
            if (File.Exists(ravenDebugExec) == false)
                throw new FileNotFoundException($"Could not find debugger tool at '{ravenDebugExec}'");

            using (var currentProcess = Process.GetCurrentProcess())
            {
                var sb = new StringBuilder($"stack-traces --pid {currentProcess.Id}");

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

                var process = new Process
                {
                    StartInfo = new ProcessStartInfo
                    {
                        Arguments = sb.ToString(),
                        FileName = ravenDebugExec,
                        WindowStyle = ProcessWindowStyle.Normal,
                        LoadUserProfile = false,
                        RedirectStandardError = true,
                        RedirectStandardOutput = true,
                        UseShellExecute = false
                    },
                    EnableRaisingEvents = true
                };

                sb.Clear();
                process.OutputDataReceived += (sender, args) => sw.Write(args.Data);
                process.ErrorDataReceived += (sender, args) => sb.Append(args.Data);

                process.Start();

                process.BeginErrorReadLine();
                process.BeginOutputReadLine();

                process.WaitForExit();

                if (process.ExitCode != 0)
                    throw new InvalidOperationException("Could not read stack traces, " +
                                                        $"exit code: {process.ExitCode}, error: {sb}");
            }
        }
    }
}
