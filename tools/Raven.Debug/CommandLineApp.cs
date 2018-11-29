using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Diagnostics.Runtime;
using Microsoft.Diagnostics.Runtime.Interop;
using Microsoft.Extensions.CommandLineUtils;
using Newtonsoft.Json;

namespace Raven.Debug
{
    internal class CommandLineApp
    {
        private const string HelpOptionString = "-h | -? | --help";

        private static CommandLineApplication _app;

        private const int LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR = 0x00000100;

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern IntPtr LoadLibraryEx(string lpFileName, IntPtr hReservedNull, int dwFlags);

        private static void EnsureProperDebugDllsAreLoaded()
        {
            var systemDirectory = Environment.GetFolderPath(Environment.SpecialFolder.System);

            var res = LoadLibraryEx(Path.Combine(systemDirectory, "dbghelp.dll"), IntPtr.Zero, LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR);
            if (res == IntPtr.Zero)
                throw new Win32Exception(Marshal.GetLastWin32Error());

            res = LoadLibraryEx(Path.Combine(systemDirectory, "dbgeng.dll"), IntPtr.Zero, LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR);
            if (res == IntPtr.Zero)
                throw new Win32Exception(Marshal.GetLastWin32Error());
        }

        public static int Run(string[] args)
        {
            if (args == null)
                throw new ArgumentNullException(nameof(args));

            EnsureProperDebugDllsAreLoaded();

            _app = new CommandLineApplication
            {
                Name = "Raven.Debug",
                Description = "Debugging tool from RavenDB"
            };

            _app.HelpOption(HelpOptionString);

            _app.Command("stack-traces", cmd =>
            {
                cmd.ExtendedHelpText = cmd.Description = "Prints stack traces for the given process.";
                cmd.HelpOption(HelpOptionString);

                var pidOption = cmd.Option("--pid", "Process ID to which the tool will attach to", CommandOptionType.SingleValue);
                var attachTimeoutOption = cmd.Option("--timeout", "Attaching to process timeout in milliseconds. Default 15000", CommandOptionType.SingleValue);
                var outputOption = cmd.Option("--output", "Output file path", CommandOptionType.SingleValue);
                var threadIdsOption = cmd.Option("--tid", "Thread ID to get the info about", CommandOptionType.MultipleValue);
                var includeStackObjectsOption = cmd.Option("--includeStackObjects", "Include the stack objects", CommandOptionType.NoValue);

                cmd.OnExecute(() =>
                {
                    if (pidOption.HasValue() == false)
                        return ExitWithError("Missing --pid option.", cmd);

                    if (int.TryParse(pidOption.Value(), out var pid) == false)
                        return ExitWithError($"Could not parse --pid with value '{pidOption.Value()}' to number.", cmd);

                    HashSet<uint> threadIds = null;
                    if (threadIdsOption.HasValue())
                    {
                        foreach (var tid in threadIdsOption.Values)
                        {
                            if (uint.TryParse(tid, out var tidAsInt) == false)
                                return ExitWithError($"Could not parse --tid with value '{tid}' to number.", cmd);

                            if (threadIds == null)
                                threadIds = new HashSet<uint>();

                            threadIds.Add(tidAsInt);
                        }
                    }

                    uint attachTimeout = 15000;
                    if (attachTimeoutOption.HasValue() && uint.TryParse(attachTimeoutOption.Value(), out attachTimeout) == false)
                        return ExitWithError($"Could not parse --attachTimeout with value '{attachTimeoutOption.Value()}' to number.", cmd);

                    string output = null;
                    if (outputOption.HasValue())
                        output = outputOption.Value();

                    var includeStackObjects = includeStackObjectsOption.Values.FirstOrDefault() == "on";

                    try
                    {
                        ShowStackTrace(pid, attachTimeout, output, cmd, threadIds, includeStackObjects);
                        return 0;
                    }
                    catch (Exception e)
                    {
                        return ExitWithError($"Failed to show the stacktrace. Error: {e}", cmd);
                    }
                });
            });

            _app.OnExecute(() =>
            {
                _app.ShowHelp();
                return 1;
            });

            try
            {
                return _app.Execute(args);
            }
            catch (CommandParsingException parsingException)
            {
                return ExitWithError(parsingException.Message, _app);
            }
        }

        private static int ExitWithError(string errMsg, CommandLineApplication cmd)
        {
            cmd.Error.WriteLine(errMsg);
            cmd.ShowHelp();
            return -1;
        }

        private static void ShowStackTrace(
            int processId, uint attachTimeout, string outputPath, 
            CommandLineApplication cmd, HashSet<uint> threadIds = null, bool includeStackObjects = false)
        {
            if (processId == -1)
                throw new InvalidOperationException("Uninitialized process id parameter");

            var threadInfoList = new List<ThreadInfo>();

            using (var dataTarget = DataTarget.AttachToProcess(processId, attachTimeout))
            {
                var clrInfo = dataTarget.ClrVersions[0];
                var runtime = clrInfo.CreateRuntime();
                var sb = new StringBuilder(1024 * 1024);
                var count = 0;

                foreach (var thread in runtime.Threads)
                {
                    if (thread.IsAlive == false)
                        continue;

                    if (threadIds != null && threadIds.Contains(thread.OSThreadId) == false)
                        continue;

                    var threadInfo = GetThreadInfo(thread, dataTarget, runtime, sb, includeStackObjects);
                    threadInfoList.Add(threadInfo);

                    count++;
                    if (threadIds != null && count == threadIds.Count)
                        break;
                }
            }

            if (threadIds != null || includeStackObjects)
            {
                OutputResult(outputPath, cmd, threadInfoList);
                return;
            }

            var mergedStackTraces = new List<StackInfo>();

            foreach (var threadInfo in threadInfoList)
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
            
            OutputResult(outputPath, cmd, mergedStackTraces);
        }

        private static void OutputResult(string outputPath, CommandLineApplication cmd, object results)
        {
            var jsonSerializer = new JsonSerializer
            {
                Formatting = Formatting.Indented
            };

            var result = new
            {
                Results = results
            };

            if (outputPath != null)
            {
                using (var output = File.Create(outputPath))
                using (var streamWriter = new StreamWriter(output))
                {
                    jsonSerializer.Serialize(streamWriter, result);
                }
            }
            else
            {
                jsonSerializer.Serialize(cmd.Out, result);
            }
        }

        private static ThreadInfo GetThreadInfo(ClrThread thread, DataTarget dataTarget, 
            ClrRuntime runtime, StringBuilder sb, bool includeStackObjects)
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
            else
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
