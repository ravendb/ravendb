using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Diagnostics.Runtime;
using Microsoft.Diagnostics.Runtime.Interop;
using Newtonsoft.Json;

namespace Raven.Debug.StackTrace
{
    public static class StackTracer
    {
        public static void ShowStackTraceWithSnapshot(int processId, TextWriter outputWriter)
        {
            if (processId == -1)
                throw new InvalidOperationException("Uninitialized process id parameter");

            List<ThreadInfo> threadInfos;
            using (var dataTarget = DataTarget.CreateSnapshotAndAttach(processId))
                threadInfos = CreateThreadInfos(dataTarget);

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
            if (processId == -1)
                throw new InvalidOperationException("Uninitialized process id parameter");

            List<ThreadInfo> threadInfos;
            using (var dataTarget = DataTarget.AttachToProcess(processId, attachTimeout, AttachFlag.Passive))
                threadInfos = CreateThreadInfos(dataTarget, threadIds, includeStackObjects);

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

        private static List<ThreadInfo> CreateThreadInfos(DataTarget dataTarget, HashSet<uint> threadIds = null, bool includeStackObjects = false)
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
                    var threadInfo = GetThreadInfo(thread, dataTarget, runtime, sb, includeStackObjects);
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
        }

        private static ThreadInfo GetThreadInfo(ClrThread thread, DataTarget dataTarget, ClrRuntime runtime, StringBuilder sb, bool includeStackObjects)
        {
            var hasStackTrace = thread.StackTrace.Count > 0;

            var threadInfo = new ThreadInfo
            {
                OSThreadId = thread.OSThreadId,
                ManagedThreadId = thread.ManagedThreadId,
                IsNative = hasStackTrace == false,
                ThreadType = thread.IsGC ? ThreadType.GC :
                    thread.IsFinalizer ? ThreadType.Finalizer :
                    hasStackTrace == false ? ThreadType.Native : ThreadType.Other
            };

            if (hasStackTrace)
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
    }
}
