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

            _app.Command("stacktraces", cmd =>
            {
                cmd.ExtendedHelpText = cmd.Description = "Print stacktraces of the attached process.";
                cmd.HelpOption(HelpOptionString);

                var pidOption = cmd.Option("--pid", "Process ID to which the tool will attach", CommandOptionType.SingleValue);
                var attachTimeoutOption = cmd.Option("--timeout", "Attaching to process timeout in milliseconds. Default 15000", CommandOptionType.SingleValue);
                var outputOption = cmd.Option("--output", "Output file path", CommandOptionType.SingleValue);

                cmd.OnExecute(() =>
                {
                    if (pidOption.HasValue() == false)
                        return ExitWithError("Missing --pid option.", cmd);

                    if (int.TryParse(pidOption.Value(), out int pid) == false)
                        return ExitWithError($"Could not parse --pid with value '{pidOption.Value()}' to number.", cmd);

                    uint attachTimeout = 15000;
                    if (attachTimeoutOption.HasValue() && uint.TryParse(attachTimeoutOption.Value(), out attachTimeout) == false)
                        return ExitWithError($"Could not parse --attachTimeout with value '{attachTimeoutOption.Value()}' to number.", cmd);

                    string output = null;
                    if (outputOption.HasValue())
                        output = outputOption.Value();

                    try
                    {
                        ShowStackTrace(pid, attachTimeout, output, cmd);
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

        private static void ShowStackTrace(int processId, uint attachTimeout, string outputPath, CommandLineApplication cmd)
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

                        control.GetStackTrace(0, 0, 0, nativeFrames, 100, out var frameCount);

                        for (int i = 0; i < frameCount; i++)
                        {
                            sb.Clear();
                            sybSymbols.GetNameByOffset(nativeFrames[i].InstructionOffset, sb, sb.Capacity, out _, out _);

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

            var jsonSerializer = new JsonSerializer
            {
                Formatting = Formatting.Indented
            };

            var result = new
            {
                Results = mergedStackTraces
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
    }
}
