using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Diagnostics.NETCore.Client;
using Microsoft.Diagnostics.Runtime;
using Microsoft.Diagnostics.Runtime.Interop;
using Microsoft.Diagnostics.Symbols;
using Microsoft.Diagnostics.Tracing;
using Microsoft.Diagnostics.Tracing.Etlx;
using Microsoft.Diagnostics.Tracing.Stacks;
using Newtonsoft.Json;

namespace Raven.Debug.StackTrace
{
    public static class StackTracer
    {
        public static void ShowStackTraceWithSnapshot(int processId, TextWriter outputWriter)
        {
            if (processId == -1)
                throw new InvalidOperationException("Uninitialized process id parameter");

            var stackTracesByThread = GetStackTracesByThreadId(processId);

            List<ThreadInfo> threadInfos;
            using (var dataTarget = DataTarget.CreateSnapshotAndAttach(processId))
                threadInfos = CreateThreadInfos(dataTarget, stackTracesByThread);

            var mergedStackTraces = MergeStackTraces(threadInfos);

            OutputResult(outputWriter, mergedStackTraces);
        }

        public static void ShowStackTrace(
            int processId,
            uint attachTimeout,
            string outputPath,
            CommandLineApplication cmd,
            HashSet<uint> threadIds = null,
            bool includeStackObjects = false)
        {
            List<ThreadInfo> threadInfos;
            var stackTracesByThread = GetStackTracesByThreadId(processId);

            using (var dataTarget = DataTarget.AttachToProcess(processId, attachTimeout, AttachFlag.Passive))
                threadInfos = CreateThreadInfos(dataTarget, stackTracesByThread, threadIds, includeStackObjects);

            if (threadIds != null || includeStackObjects)
            {
                using (GetOutputWriter(outputPath, cmd, out var outputWriter))
                    OutputResult(outputWriter, threadInfos);
                return;
            }

            var mergedStackTraces = MergeStackTraces(threadInfos);

            using (GetOutputWriter(outputPath, cmd, out var outputWriter))
                OutputResult(outputWriter, mergedStackTraces);

            static IDisposable GetOutputWriter(string outputPath, CommandLineApplication cmd, out TextWriter outputWriter)
            {
                if (outputPath != null)
                {
                    var output = File.Create(outputPath);
                    outputWriter = new StreamWriter(output);
                    return outputWriter;
                }

                outputWriter = cmd.Out;
                return null;
            }
        }

        private static List<ThreadInfo> CreateThreadInfos(DataTarget dataTarget, Dictionary<uint, List<string>> stackTracesByThreadId, HashSet<uint> threadIds = null, bool includeStackObjects = false)
        {
            var threadInfoList = new List<ThreadInfo>();

            var clrInfo = dataTarget.ClrVersions[0];
            ClrRuntime runtime;
            try
            {
                runtime = clrInfo.CreateRuntime();
            }
            catch (Exception)
            {
                var path = Path.Combine(AppContext.BaseDirectory, clrInfo.DacInfo.FileName);
                runtime = clrInfo.CreateRuntime(path);
            }

            var sb = new StringBuilder(1024 * 1024);
            var count = 0;

            foreach (var thread in runtime.Threads)
            {
                if (thread.IsAlive == false)
                    continue;

                if (threadIds != null && threadIds.Contains(thread.OSThreadId) == false)
                    continue;

                try
                {
                    stackTracesByThreadId.TryGetValue(thread.OSThreadId, out var stackTraces);
                    var threadInfo = GetThreadInfo(stackTraces, thread, dataTarget, runtime, sb, includeStackObjects);
                    threadInfoList.Add(threadInfo);
                }
                catch (InvalidOperationException)
                {
                    // thread has exited
                    continue;
                }
                catch (Win32Exception e) when (e.HResult == 0x5)
                {
                    // thread has exited
                    continue;
                }

                count++;
                if (threadIds != null && count == threadIds.Count)
                    break;
            }

            return threadInfoList;
        }

        private static List<StackInfo> MergeStackTraces(List<ThreadInfo> stackTraces)
        {
            var mergedStackTraces = new List<StackInfo>();

            foreach (var threadInfo in stackTraces)
            {
                var merged = false;

                foreach (var mergedStack in mergedStackTraces)
                {
                    if (threadInfo.IsNative != mergedStack.NativeThreads)
                        continue;

                    if (threadInfo.StackTrace.SequenceEqual(mergedStack.StackTrace, StringComparer.OrdinalIgnoreCase) == false)
                        continue;

                    if (mergedStack.ThreadIds.Contains(threadInfo.OSThreadId) == false)
                        mergedStack.ThreadIds.Add(threadInfo.OSThreadId);

                    merged = true;
                    break;
                }

                if (merged)
                    continue;

                mergedStackTraces.Add(new StackInfo
                {
                    ThreadIds = new List<uint>
                    {
                        threadInfo.OSThreadId
                    },
                    StackTrace = threadInfo.StackTrace,
                    NativeThreads = threadInfo.IsNative
                });
            }

            return mergedStackTraces;
        }

        private static void OutputResult(TextWriter outputWriter, object results)
        {
            var jsonSerializer = new JsonSerializer
            {
                Formatting = Formatting.Indented
            };

            var result = new
            {
                Results = results
            };

            jsonSerializer.Serialize(outputWriter, result);

            outputWriter.Flush();
        }

        private static ThreadInfo GetThreadInfo(List<string> stackTraces, ClrThread thread, DataTarget dataTarget, ClrRuntime runtime, StringBuilder sb, bool includeStackObjects)
        {
            var hasStackTrace = stackTraces?.Count > 0 || thread.StackTrace.Count > 0;

            var threadInfo = new ThreadInfo
            {
                OSThreadId = thread.OSThreadId,
                ManagedThreadId = thread.ManagedThreadId,
                IsNative = hasStackTrace == false,
                ThreadType = thread.IsGC ? ThreadType.GC :
                    thread.IsFinalizer ? ThreadType.Finalizer :
                    hasStackTrace == false ? ThreadType.Native : ThreadType.Other
            };

            if (stackTraces?.Count > 0)
            {
                threadInfo.StackTrace = stackTraces;
            }
            else if (thread.StackTrace.Count > 0)
            {
                foreach (var frame in thread.StackTrace)
                {
                    if (frame.DisplayString.Equals("GCFrame", StringComparison.OrdinalIgnoreCase) ||
                        frame.DisplayString.Equals("DebuggerU2MCatchHandlerFrame", StringComparison.OrdinalIgnoreCase))
                        continue;

                    threadInfo.StackTrace.Add(frame.DisplayString);
                }
            }
            else if (dataTarget.DebuggerInterface != null)
            {
                var control = (IDebugControl)dataTarget.DebuggerInterface;
                var sysObjs = (IDebugSystemObjects)dataTarget.DebuggerInterface;
                var nativeFrames = new DEBUG_STACK_FRAME[100];
                var sybSymbols = (IDebugSymbols)dataTarget.DebuggerInterface;

                threadInfo.IsNative = true;

                sysObjs.SetCurrentThreadId(threadInfo.OSThreadId);

                control.GetStackTrace(0, 0, 0, nativeFrames, 100, out var frameCount);

                for (var i = 0; i < frameCount; i++)
                {
                    sb.Clear();
                    sybSymbols.GetNameByOffset(nativeFrames[i].InstructionOffset, sb, sb.Capacity, out _, out _);

                    threadInfo.StackTrace.Add(sb.ToString());
                }
            }

            if (includeStackObjects)
            {
                threadInfo.StackObjects = GetStackObjects(runtime, thread);
            }

            return threadInfo;
        }

        private static List<string> GetStackObjects(ClrRuntime runtime, ClrThread thread)
        {
            var stackObjects = new List<string>();

            var heap = runtime.Heap;

            // Walk each pointer aligned address on the stack.  Note that StackBase/StackLimit
            // is exactly what they are in the TEB.  This means StackBase > StackLimit on AMD64.
            var start = thread.StackBase;
            var stop = thread.StackLimit;

            // We'll walk these in pointer order.
            if (start > stop)
            {
                var tmp = start;
                start = stop;
                stop = tmp;
            }

            // Walk each pointer aligned address.  Ptr is a stack address.
            for (var ptr = start; ptr <= stop; ptr += (ulong)runtime.PointerSize)
            {
                // Read the value of this pointer.  If we fail to read the memory, break.  The
                // stack region should be in the crash dump.
                if (runtime.ReadPointer(ptr, out var obj) == false)
                    break;

                // 003DF2A4 
                // We check to see if this address is a valid object by simply calling
                // GetObjectType.  If that returns null, it's not an object.
                var type = heap.GetObjectType(obj);
                if (type == null)
                    continue;

                // Don't print out free objects as there tends to be a lot of them on
                // the stack.
                if (type.IsFree)
                    continue;

                stackObjects.Add(type.Name);
            }

            return stackObjects;
        }

        private static Dictionary<uint, List<string>> GetStackTracesByThreadId(int processId, HashSet<uint> threadIds = null)
        {
            var stackTracesByThreadId = new Dictionary<uint, List<string>>();

            string tempNetTraceFilename = Path.GetRandomFileName() + ".nettrace";
            string tempEtlxFilename = "";

            try
            {
                var client = new DiagnosticsClient(processId);
                var providers = new List<EventPipeProvider>
                {
                    new EventPipeProvider("Microsoft-DotNETCore-SampleProfiler", EventLevel.Informational)
                };

                // collect a *short* trace with stack samples
                // the hidden '--duration' flag can increase the time of this trace in case 10ms
                // is too short in a given environment, e.g., resource constrained systems
                // N.B. - This trace INCLUDES rundown.  For sufficiently large applications, it may take non-trivial time to collect
                //        the symbol data in rundown.
                using (EventPipeSession session = client.StartEventPipeSession(providers))
                using (FileStream fs = File.OpenWrite(tempNetTraceFilename))
                {
                    Task copyTask = session.EventStream.CopyToAsync(fs);
                    session.Stop();

                    // check if rundown is taking more than 5 seconds and add comment to report
                    Task timeoutTask = Task.Delay(TimeSpan.FromSeconds(5));
                    Task completedTask = Task.WhenAny(copyTask, timeoutTask).Result;
                    if (completedTask == timeoutTask)
                    {
                        Console.WriteLine($"# Sufficiently large applications can cause this command to take non-trivial amounts of time");
                    }

                    copyTask.Wait();
                }

                // using the generated trace file, symbolocate and compute stacks.
                tempEtlxFilename = TraceLog.CreateFromEventPipeDataFile(tempNetTraceFilename);
                using (var symbolReader = new SymbolReader(TextWriter.Null)
                {
                    SymbolPath = SymbolPath.MicrosoftSymbolServerPath
                })
                using (var eventLog = new TraceLog(tempEtlxFilename))
                {
                    var stackSource = new MutableTraceEventStackSource(eventLog)
                    {
                        OnlyManagedCodeStacks = true
                    };

                    var computer = new SampleProfilerThreadTimeComputer(eventLog, symbolReader);
                    computer.GenerateThreadTimeStacks(stackSource);

                    var samplesForThread = new Dictionary<uint, List<StackSourceSample>>();

                    stackSource.ForEach(sample =>
                    {
                        var stackIndex = sample.StackIndex;
                        while (stackSource.GetFrameName(stackSource.GetFrameIndex(stackIndex), false).StartsWith("Thread (") == false)
                            stackIndex = stackSource.GetCallerIndex(stackIndex);

                        // long form for: int.Parse(threadFrame["Thread (".Length..^1)])
                        // Thread id is in the frame name as "Thread (<ID>)"
                        const string template = "Thread (";
                        string threadFrame = stackSource.GetFrameName(stackSource.GetFrameIndex(stackIndex), false);
                        var threadId = uint.Parse(threadFrame.Substring(template.Length, threadFrame.Length - (template.Length + 1)));

                        if (threadIds != null && threadIds.Contains(threadId) == false)
                            return;

                        if (samplesForThread.TryGetValue(threadId, out var samples))
                        {
                            samples.Add(sample);
                        }
                        else
                        {
                            samplesForThread[threadId] = new List<StackSourceSample> { sample };
                        }
                    });

                    foreach (var (threadId, samples) in samplesForThread)
                    {
                        var stack = GetStack(samples[0], stackSource);
                        stackTracesByThreadId[threadId] = stack;
                    }
                }
            }
            finally
            {
                if (File.Exists(tempNetTraceFilename))
                    File.Delete(tempNetTraceFilename);
                if (File.Exists(tempEtlxFilename))
                    File.Delete(tempEtlxFilename);
            }

            return stackTracesByThreadId;
        }

        private static List<string> GetStack(StackSourceSample stackSourceSample, StackSource stackSource)
        {
            var stackTrace = new List<string>();
            var stackIndex = stackSourceSample.StackIndex;

            while (stackSource.GetFrameName(stackSource.GetFrameIndex(stackIndex), verboseName: false).StartsWith("Thread (") == false)
            {
                var frame = $"  {stackSource.GetFrameName(stackSource.GetFrameIndex(stackIndex), verboseName: false)}"
                    .Replace("UNMANAGED_CODE_TIME", "[Native Frames]");

                stackTrace.Add(frame);
                stackIndex = stackSource.GetCallerIndex(stackIndex);
            }

            return stackTrace;
        }
    }
}
