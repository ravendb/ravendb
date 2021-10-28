using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Diagnostics.Tools.Dump;
using Microsoft.Diagnostics.Tools.GCDump;
using Raven.Client.Documents.Changes;
using Raven.Debug.StackTrace;
using Raven.Debug.Utils;

namespace Raven.Debug
{
    internal class CommandLineApp
    {
        private const string HelpOptionString = "-h | -? | --help";

        private static CommandLineApplication _app;

        private const int LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR = 0x00000100;

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern IntPtr LoadLibraryEx(string lpFileName, IntPtr hReservedNull, int dwFlags);

        private static void EnsureProperDebugDllsAreLoadedForWindows()
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

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                EnsureProperDebugDllsAreLoadedForWindows();

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

                var waitOption = cmd.Option("--wait", "Wait for user input", CommandOptionType.NoValue);
                var pidOption = cmd.Option("--pid", "Process ID to which the tool will attach to", CommandOptionType.SingleValue);
                var attachTimeoutOption = cmd.Option("--timeout", "Attaching to process timeout in milliseconds. Default 15000", CommandOptionType.SingleValue);
                var outputOption = cmd.Option("--output", "Output file path", CommandOptionType.SingleValue);
                var threadIdsOption = cmd.Option("--tid", "Thread ID to get the info about", CommandOptionType.MultipleValue);
                var includeStackObjectsOption = cmd.Option("--includeStackObjects", "Include the stack objects", CommandOptionType.NoValue);

                cmd.OnExecute(() =>
                {
                    if (waitOption.HasValue())
                        Console.ReadLine(); // wait for the caller to finish preparing for us

                    if (pidOption.HasValue() == false)
                        return cmd.ExitWithError("Missing --pid option.");

                    if (int.TryParse(pidOption.Value(), out var pid) == false)
                        return cmd.ExitWithError($"Could not parse --pid with value '{pidOption.Value()}' to number.");

                    if (pid <= 0)
                        return cmd.ExitWithError("Process ID must be greater than 0");

                    HashSet<uint> threadIds = null;
                    if (threadIdsOption.HasValue())
                    {
                        foreach (var tid in threadIdsOption.Values)
                        {
                            if (uint.TryParse(tid, out var tidAsInt) == false)
                                return cmd.ExitWithError($"Could not parse --tid with value '{tid}' to number.");

                            if (threadIds == null)
                                threadIds = new HashSet<uint>();

                            threadIds.Add(tidAsInt);
                        }
                    }

                    uint attachTimeout = 15000;
                    if (attachTimeoutOption.HasValue() && uint.TryParse(attachTimeoutOption.Value(), out attachTimeout) == false)
                        return cmd.ExitWithError($"Could not parse --attachTimeout with value '{attachTimeoutOption.Value()}' to number.");

                    string output = null;
                    if (outputOption.HasValue())
                        output = outputOption.Value();

                    var includeStackObjects = includeStackObjectsOption.Values.FirstOrDefault() == "on";

                    try
                    {
                        StackTracer.ShowStackTrace(pid, attachTimeout, output, cmd, threadIds, includeStackObjects);
                        return 0;
                    }
                    catch (Exception e)
                    {
                        string desc;
                        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) == false)
                            desc = "";
                        else
                        {
                            desc = $"Make sure to run enable-debugging.sh script as root from the main RavenDB directory.";
                        }

                        return cmd.ExitWithError($"Failed to show the stacktrace. {desc}Error: {e}");
                    }
                });
            });

            _app.Command("dump", cmd =>
            {
                cmd.ExtendedHelpText = cmd.Description = "Creates dump for the given process.";
                cmd.HelpOption(HelpOptionString);

                var pidOption = cmd.Option("--pid", "Process ID to which the tool will attach to", CommandOptionType.SingleValue);
                var outputOption = cmd.Option("--output", "Output file path", CommandOptionType.SingleValue);
                var typeOption = cmd.Option("--type", "Type of dump (Heap or Mini). ", CommandOptionType.SingleValue);

                cmd.OnExecuteAsync(async (_) =>
                {
                    if (pidOption.HasValue() == false)
                        return cmd.ExitWithError("Missing --pid option.");

                    if (int.TryParse(pidOption.Value(), out var pid) == false)
                        return cmd.ExitWithError($"Could not parse --pid with value '{pidOption.Value()}' to number.");

                    if (typeOption.HasValue() == false)
                        return cmd.ExitWithError("Missing --type option.");

                    if (Enum.TryParse(typeOption.Value(), ignoreCase: true, out Dumper.DumpTypeOption type) == false)
                        return cmd.ExitWithError($"Could not parse --type with value '{typeOption.Value()}' to one of supported dump types.");

                    string output = null;
                    if (outputOption.HasValue())
                        output = outputOption.Value();

                    try
                    {
                        var dumper = new Dumper();
                        await dumper.Collect(cmd, pid, output, diag: false, type).ConfigureAwait(false);
                        return 0;
                    }
                    catch (Exception e)
                    {
                        return cmd.ExitWithError($"Failed to collect dump. Error: {e}");
                    }
                });
            });

            _app.Command("gcdump", cmd =>
            {
                cmd.ExtendedHelpText = cmd.Description = "Creates GC dump for the given process.";
                cmd.HelpOption(HelpOptionString);

                var pidOption = cmd.Option("--pid", "Process ID to which the tool will attach to", CommandOptionType.SingleValue);
                var outputOption = cmd.Option("--output", "Output file path", CommandOptionType.SingleValue);
                var timeoutOption = cmd.Option("--timeout", "Give up on collecting the gcdump if it takes longer than this many seconds. The default value is. Default 30", CommandOptionType.SingleValue);
                var verboseOption = cmd.Option("--verbose", "Output the log while collecting the gcdump.", CommandOptionType.NoValue);

                cmd.OnExecuteAsync(async token =>
                {
                    if (pidOption.HasValue() == false)
                        return cmd.ExitWithError("Missing --pid option.");

                    if (int.TryParse(pidOption.Value(), out var pid) == false)
                        return cmd.ExitWithError($"Could not parse --pid with value '{pidOption.Value()}' to number.");

                    string output = null;
                    if (outputOption.HasValue())
                        output = outputOption.Value();

                    int timeout = 30;
                    if (timeoutOption.HasValue() && int.TryParse(timeoutOption.Value(), out timeout) == false)
                        return cmd.ExitWithError($"Could not parse --timeout with value '{timeoutOption.Value()}' to number.");

                    var verbose = verboseOption.HasValue();

                    try
                    {
                        await GCHeapDumper.Collect(token, cmd, pid, output, timeout, verbose).ConfigureAwait(false);
                        return 0;
                    }
                    catch (Exception e)
                    {
                        return cmd.ExitWithError($"Failed to collect GC dump. Error: {e}");
                    }
                });
            });

            ConfigureTrafficWatchCommand();

            _app.OnExecute(() =>
            {
                _app.ShowHelp();
                return 1;
            });

            try
            {
                return _app.Execute(args);
            }
            catch (CommandParsingException e)
            {
                return _app.ExitWithError(e.Message);
            }
        }

        private static void ConfigureTrafficWatchCommand()
        {
            _app.Command("traffic", cmd =>
            {
                cmd.ExtendedHelpText = cmd.Description = "Log traffic watch entries.";
                cmd.HelpOption(HelpOptionString);

                var urlArg = cmd.Option("--url", "Server URL.", CommandOptionType.SingleValue);
                var pathArg = cmd.Option("--output-path", "Directory path to save the traffic watch log.", CommandOptionType.SingleValue);
                var databaseArg = cmd.Option("--database", "The database name.", CommandOptionType.SingleOrNoValue);
                var types = Enum.GetValues(typeof(TrafficWatchChangeType)).Cast<TrafficWatchChangeType>().Select(t => t.ToString());
                var typesArg = cmd.Option("--types", $"Types to be filtered by - {string.Join(", ",types)}.", CommandOptionType.MultipleValue);
                var certArg = cmd.Option("--certificate-path", "Path to pfx certificate file.", CommandOptionType.SingleOrNoValue);
                var certPassArg = cmd.Option("--certificate-password", "Certificate password.", CommandOptionType.SingleOrNoValue);
                var verboseArg = cmd.Option("--verbose", "Verbose to console.", CommandOptionType.NoValue);

                cmd.OnExecute(() =>
                {
                    string path;
                    string url;

                    if (urlArg.HasValue())
                    {
                        url = urlArg.Value();
                    }
                    else
                    {
                        cmd.ExitWithError("Url argument is invalid.");
                        return -1;
                    }

                    if (pathArg.HasValue())
                    {
                        path = pathArg.Value();
                    }
                    else
                    {
                        cmd.ExitWithError("Path argument is invalid.");
                        return -1;
                    }

                    List<TrafficWatchChangeType> changeTypes = null;
                    if (typesArg.HasValue())
                    {
                        changeTypes = new List<TrafficWatchChangeType>();
                        try
                        {
                            StringBuilder error = null;
                            foreach (var type in typesArg.Values)
                            {
                                if (Enum.TryParse(type, out TrafficWatchChangeType parsed))
                                {
                                    changeTypes.Add(parsed);
                                }
                                else
                                {
                                    error ??= new StringBuilder();
                                    error.AppendLine("Types argument is invalid.");
                                }
                            }

                            if(error != null)
                                cmd.ExitWithError(error.ToString());
                        }
                        catch
                        {
                            cmd.ExitWithError("Types argument is invalid.");
                        }
                    }

                    string cert = certArg.HasValue() ? certArg.Value() :null;
                    string certPass = certPassArg.HasValue() ? certPassArg.Value() :null;
                    string database = databaseArg.HasValue() ? databaseArg.Value() :null;

                    var  logTrafficWatch = new LogTrafficWatch.LogTrafficWatch(path, url, cert, certPass, database, changeTypes?.ToArray(), verboseArg.HasValue());

                    Console.CancelKeyPress += (sender, args) => logTrafficWatch.Stop();

                    using (logTrafficWatch)
                        logTrafficWatch.Connect().Wait();

                    return 0;
                });
            });
        }

    }
}
