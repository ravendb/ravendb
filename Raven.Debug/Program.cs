using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

using Microsoft.Diagnostics.Runtime;
using Microsoft.Diagnostics.Runtime.Interop;

using NDesk.Options;

using Newtonsoft.Json;

namespace Raven.Debug
{
    public class Program
    {
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

        public static int Main(string[] args)
        {
            int processId = -1;
            uint attachTimeout = 15000;
            Action actionToTake = null;
            string outputFilePath = null;

            var optionSet = new OptionSet
            {
                {"pid=", OptionCategory.General, "Process id.", pid => processId = int.Parse(pid)},
                {"attachTimeout=", OptionCategory.General, "Attaching to process timeout in miliseconds. Default 15000.", timeout => attachTimeout = uint.Parse(timeout)},
                {"output=", OptionCategory.General, "Output file path.", path => outputFilePath = path},
                {"stacktrace", OptionCategory.General, "Print stacktraces of the attached process.", x => actionToTake = () => ShowStackTrace(processId, attachTimeout, outputFilePath)}
            };

            try
            {
                if (args.Length == 0)
                {
                    PrintUsage(optionSet);
                    return -2;
                }

                optionSet.Parse(args);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.Message);
                PrintUsage(optionSet);
                return -2;
            }

            EnsureProperDebugDllsAreLoaded();

            try
            {
                actionToTake?.Invoke();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.ToString());
                return -1;
            }

            return 0;
        }

        private static void ShowStackTrace(int processId, uint attachTimeout, string outputPath)
        {
            if (processId == -1)
                throw new InvalidOperationException("Uninitialized process id parameter");

            var threadInfoList = new List<ThreadInfo>();

            using (DataTarget dataTarget = DataTarget.AttachToProcess(processId, attachTimeout))
            {
                var clrInfo = dataTarget.ClrVersions[0];
                var runtime = clrInfo.CreateRuntime();
                var control = (IDebugControl)dataTarget.DebuggerInterface;
                var sysObjs = (IDebugSystemObjects)dataTarget.DebuggerInterface;
                var nativeFrames = new DEBUG_STACK_FRAME[100];
                var sybSymbols = (IDebugSymbols)dataTarget.DebuggerInterface;

                var sb = new StringBuilder(1024 * 1024);

                foreach (ClrThread thread in runtime.Threads)
                {

                    var threadInfo = new ThreadInfo
                    {
                        OSThreadId = thread.OSThreadId
                    };

                    if (thread.StackTrace.Count > 0)
                    {
                        foreach (ClrStackFrame frame in thread.StackTrace)
                        {
                            if (frame.DisplayString.Equals("GCFrame") || frame.DisplayString.Equals("DebuggerU2MCatchHandlerFrame"))
                                continue;

                            threadInfo.StackTrace.Add(frame.DisplayString);
                        }
                    }
                    else
                    {
                        threadInfo.IsNative = true;

                        sysObjs.SetCurrentThreadId(threadInfo.OSThreadId);

                        uint frameCount;
                        control.GetStackTrace(0, 0, 0, nativeFrames, 100, out frameCount);

                        for (int i = 0; i < frameCount; i++)
                        {
                            uint nameSize;
                            ulong dis;

                            sb.Clear();
                            sybSymbols.GetNameByOffset(nativeFrames[i].InstructionOffset, sb, sb.Capacity, out nameSize, out dis);

                            threadInfo.StackTrace.Add(sb.ToString());
                        }
                    }

                    threadInfoList.Add(threadInfo);
                }
            }

            var mergedStackTraces = new List<StackInfo>();

            foreach (var threadInfo in threadInfoList)
            {
                bool merged = false;

                foreach (var mergedStack in mergedStackTraces)
                {
                    if (threadInfo.IsNative != mergedStack.NativeThreads)
                        continue;

                    if (threadInfo.StackTrace.SequenceEqual(mergedStack.StackTrace, StringComparer.InvariantCultureIgnoreCase) == false)
                        continue;

                    if (mergedStack.ThreadIds.Contains(threadInfo.OSThreadId) == false)
                        mergedStack.ThreadIds.Add(threadInfo.OSThreadId);

                    merged = true;
                    break;
                }

                if (merged)
                    continue;

                mergedStackTraces.Add(new StackInfo()
                {
                    ThreadIds = new List<uint>() { threadInfo.OSThreadId },
                    StackTrace = threadInfo.StackTrace,
                    NativeThreads = threadInfo.IsNative
                });
            }

            var jsonSerializer = new JsonSerializer
            {
                Formatting = Formatting.Indented
            };

            if (outputPath != null)
            {
                using (var output = File.Create(outputPath))
                using (var streamWriter = new StreamWriter(output))
                {
                    jsonSerializer.Serialize(streamWriter, mergedStackTraces);
                }
            }
            else
            {
                jsonSerializer.Serialize(Console.Out, mergedStackTraces);
            }
        }

        private static void PrintUsage(OptionSet optionSet)
        {
            Console.WriteLine(
                @"
RavenDB
Document Database for the .Net Platform
----------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------
Command line options:",
                DateTime.UtcNow.Year);

            optionSet.WriteOptionDescriptions(Console.Out);

            Console.WriteLine(@"
Enjoy...
");
        }
    }
}
