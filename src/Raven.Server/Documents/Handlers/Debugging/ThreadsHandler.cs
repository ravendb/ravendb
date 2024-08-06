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
using Raven.Server.Dashboard;
using Raven.Server.EventListener;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ThreadsHandler : ServerRequestHandler
    {
        [RavenAction("/admin/debug/threads/stack-trace", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task StackTrace()
        {
            if (PlatformDetails.RunningOnMacOsx)
                throw new NotSupportedException("Capturing live stack traces is not supported by RavenDB on MacOSX");

            var threadIdsAsString = GetStringValuesQueryString("threadId", required: false);
            var includeStackObjects = GetBoolValueQueryString("includeStackObjects", required: false) ?? false;

            var sp = Stopwatch.StartNew();
            var threadsUsage = new ThreadsUsage();

            await using (var sw = new StringWriter())
            {
                OutputResultToStream(sw, threadIdsAsString.ToHashSet(), includeStackObjects);

                var result = JObject.Parse(sw.GetStringBuilder().ToString());

                var wait = 100 - sp.ElapsedMilliseconds;
                if (wait > 0)
                {
                    // I expect this to be _rare_, but we need to wait to get a correct measure of the cpu
                    await Task.Delay((int)wait);
                }

                var threadStats = threadsUsage.Calculate(threadIds: threadIdsAsString.Count == 0 ? null : threadIdsAsString.Select(int.Parse).ToHashSet());
                result["Threads"] = JArray.FromObject(threadStats.List);

                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
                    {
                        context.Write(writer, DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(result, context));
                    }
                }
            }
        }

        [RavenAction("/admin/debug/threads/runaway", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task RunawayThreads()
        {
            var samplesCount = GetIntValueQueryString("samplesCount", required: false) ?? 1;
            var interval = GetIntValueQueryString("intervalInMs", required: false) ?? ServerMetricCacher.DefaultCpuRefreshRateInMs;
            var maxTopThreads = GetIntValueQueryString("maxTopThreads", required: false);

            if (samplesCount <= 0)
                throw new ArgumentException("Must be positive", "samplesCount");

            if (interval <= 0)
                throw new ArgumentException("Must be positive", "interval");

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
                {
                    try
                    {
                        var threadsInfos = await GetThreadsInfos();

                        if (samplesCount == 1)
                        {
                            context.Write(writer,
                                new DynamicJsonValue
                                {
                                    ["Runaway Threads"] = threadsInfos.First().ToJson()
                                });
                            return;
                        }

                        writer.WriteStartObject();
                        writer.WritePropertyName("Results");

                        var dja = new DynamicJsonArray();

                        foreach (var threadInfo in threadsInfos)
                        {
                            dja.Add(threadInfo.ToJson());
                        }

                        context.Write(writer, dja);
                        writer.WriteEndObject();
                        
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

            async Task<List<ThreadsInfo>> GetThreadsInfos()
            {
                var results = new List<ThreadsInfo>();

                var threadsUsage = new ThreadsUsage();

                for (var i = 0; i < samplesCount; i++)
                {
                    await Task.Delay(interval, ServerStore.ServerShutdown);
                    results.Add(threadsUsage.Calculate(maxTopThreads));
                }

                return results;
            }
        }

        [RavenAction("/admin/debug/threads/contention", "GET", AuthorizationStatus.Operator,
            // intentionally not calling it debug endpoint because it isn't valid for us
            // to do so in debug package (since we force a wait)
            IsDebugInformationEndpoint = false)]
        public async Task Contention()
        {
            var delay = GetIntValueQueryString("delay", required: false) ?? 30;

            IReadOnlyCollection<ContentionEventsHandler.ContentionEvent> events;
            using (var listener = new ContentionEventsListener())
            {
                await Task.Delay(TimeSpan.FromSeconds(delay));
                events = listener.Events;
            }

            var sortedEvents = new SortedSet<ContentionEventsHandler.ContentionEvent>(events, new EventComparerByDuration());

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("TopFiveByDuration");
                writer.WriteStartArray();

                var first = true;
                var count = 0;
                foreach (var @event in sortedEvents)
                {
                    if (++count > 5)
                        break;

                    if (first == false)
                        writer.WriteComma();

                    first = false;

                    context.Write(writer, @event.ToJson());
                }

                writer.WriteEndArray();

                writer.WriteComma();
                writer.WritePropertyName("Events");
                writer.WriteStartArray();

                first = true;
                foreach (var @event in events.OrderBy(x => x.StartTime))
                {
                    if (first == false)
                        writer.WriteComma();

                    first = false;

                    context.Write(writer, @event.ToJson());
                }

                writer.WriteEndArray();

                writer.WriteEndObject();
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

                using (var process = new Process { StartInfo = startup, EnableRaisingEvents = true })
                {
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

                        process.StandardInput.WriteLine("go"); // value is meaningless, just need a new line
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
}
