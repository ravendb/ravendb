using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Client.Server.Operations.Certificates;
using Raven.Server.Documents.Handlers.Debugging;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Utils;

namespace Raven.Server.Utils
{
    internal class RavenCli
    {
        private static readonly Action<List<string>, bool, RavenServer, TextWriter> Prompt = (list, test, server, writer) =>
        {
            var msg = new StringBuilder();
            var first = true;
            foreach (var l in list)
            {
                if (first == false)
                    msg.Append(" ");
                else
                    first = false;

                switch (l)
                {
                    case "%D":
                        msg.Append(DateTime.UtcNow.ToString("yyyy/MMM/dd"));
                        break;
                    case "%T":
                        msg.Append(DateTime.UtcNow.ToString("HH:mm:ss"));
                        break;
                    case "%M":
                        {
                            var json = MemoryStatsHandler.MemoryStatsInternal();
                            var humaneProp = (json["Humane"] as DynamicJsonValue);
                            msg.Append($"WS:{humaneProp?["WorkingSet"]}");
                            msg.Append($"|UM:{humaneProp?["TotalUnmanagedAllocations"]}");
                            msg.Append($"|M:{humaneProp?["ManagedAllocations"]}");
                            msg.Append($"|MP:{humaneProp?["TotalMemoryMapped"]}");
                        }
                        break;
                    case "%R":
                        {
                            var reqCounter = server.Metrics.RequestsMeter;
                            msg.Append($"Req/Sec:{Math.Round(reqCounter.OneSecondRate, 1)}");
                        }
                        break;

                    default:
                        msg.Append(l);
                        break;
                }
            }
            if (test == false)
                writer.Write(msg);
        };

        private List<string> _promptArgs = new List<string> { "ravendb" };
        private TextWriter _writer;
        private TextReader _reader;


        private class SingleAction
        {
            public int NumOfArgs;
            public Func<List<string>, TextWriter, TextReader, bool, RavenServer, bool> DelegateFync;
            public bool Experimental { get; set; }
        }

        private static bool CommandQuit(List<string> args, TextWriter writer, TextReader reader, bool consoleColoring, RavenServer server)
        {
            ResetColor(consoleColoring);
            writer.WriteLine();
            writer.Write("Are you sure you want to quit the server ? [y/N] : ");
            writer.Flush();

            var k = ReadKey(consoleColoring, reader);
            writer.Flush();

            writer.WriteLine();
            return char.ToLower(k).Equals('y');
        }

        private static char ReadKey(bool consoleColoring, TextReader reader)
        {
            if (consoleColoring)
                return Console.ReadKey().KeyChar;

            var c = new char[1];
            while (reader.Read(c, 0, 1) < 1)
            {

            }
            return c[0];
        }

        private string ReadLine()
        {
            return _consoleColoring ? Console.ReadLine() : _reader.ReadLine();
        }

        private static bool CommandResetServer(List<string> args, TextWriter writer, TextReader reader, bool consoleColoring, RavenServer server)
        {
            ResetColor(consoleColoring);
            writer.WriteLine();
            writer.Write("Are you sure you want to reset the server ? [y/N] : ");
            writer.Flush();

            var k = ReadKey(consoleColoring, reader);
            writer.Flush();

            writer.WriteLine();
            return char.ToLower(k).Equals('y');
        }

        private static bool CommandStats(List<string> args, TextWriter writer, TextReader reader, bool consoleColoring, RavenServer server)
        {
            LoggingSource.Instance.DisableConsoleLogging();
            LoggingSource.Instance.SetupLogMode(LogMode.None,
                Path.Combine(AppContext.BaseDirectory, server.Configuration.Logs.Path));

            Program.WriteServerStatsAndWaitForEsc(server);

            return true;
        }

        private static bool CommandPrompt(List<string> args, TextWriter writer, TextReader reader, bool consoleColoring, RavenServer server)
        {
            try
            {
                Prompt.Invoke(args, true, server, writer);
            }
            catch (Exception ex)
            {
                WriteError("Cannot set prompt to desired args, because of : " + ex.Message, writer, consoleColoring);
                return false;
            }
            return true;
        }

        private static bool CommandHelpPrompt(List<string> args, TextWriter writer, TextReader reader, bool consoleColoring, RavenServer server)
        {
            string[][] commandDescription = {
                new[] {"%D", "UTC Date"},
                new[] {"%T", "UTC Time"},
                new[] {"%M", "Memory information (WS:WorkingSet, UM:Unmanaged, M:Managed, MP:MemoryMapped)"},
                new[] {"%R", "Momentary Req/Sec"},
                new[] {"label", "any label"},
            };

            ResetColor(consoleColoring);
            var msg = new StringBuilder();
            msg.Append("Usage: prompt <[label] | [ %D | %T | %M ] | ...>" + Environment.NewLine + Environment.NewLine);
            msg.Append("Options:" + Environment.NewLine);
            writer.WriteLine(msg);

            foreach (var cmd in commandDescription)
            {
                ForegroundColor(ConsoleColor.Yellow, consoleColoring);
                writer.Write("\t" + cmd[0]);
                ForegroundColor(ConsoleColor.DarkYellow, consoleColoring);
                writer.WriteLine(new string(' ', 25 - cmd[0].Length) + cmd[1]);
            }
            ResetColor(consoleColoring);
            writer.WriteLine();
            writer.Flush();
            return true;
        }
        

        private static bool CommandAddServerCert(List<string> args, TextWriter writer, TextReader reader, bool consoleColoring, RavenServer server)
        {
            var path = args.First();

            var cert = args.Count != 1 ? new X509Certificate2(path, args[1]) : new X509Certificate2(path);
            ResetColor(consoleColoring);
            writer.Write("Successfully read certificate: " + cert.Thumbprint);
            writer.Write(cert.ToString());
            var certKey = Constants.Certificates.Prefix + cert.Thumbprint;

            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var json = new CertificateDefinition
                {
                    // this does not include the private key, that is only for the client
                    Certificate = Convert.ToBase64String(cert.Export(X509ContentType.Cert)),
                    Databases = null,
                    ServerAdmin = true,
                    Thumbprint = cert.Thumbprint
                }.ToJson();

                BlittableJsonReaderObject certificate = ctx.ReadObject(json, "Server/Certificate/Definition");
                using (ctx.OpenWriteTransaction())
                {
                    server.ServerStore.Cluster.PutLocalState(ctx, certKey, certificate);
                }
            }

            return true;
        }

        private static bool CommandGc(List<string> args, TextWriter writer, TextReader reader, bool consoleColoring, RavenServer server)
        {
            var genNum = Convert.ToInt32(args.First());
            ResetColor(consoleColoring);
            writer.Write("Before collecting, managed memory used: ");
            ForegroundColor(ConsoleColor.Cyan, consoleColoring);
            writer.WriteLine(new Size(GC.GetTotalMemory(false), SizeUnit.Bytes));
            ResetColor(consoleColoring);
            var startTime = DateTime.UtcNow;
            writer.Write("Garbage Collecting... ");
            writer.Flush();

            switch (genNum)
            {
                case 0:
                    GC.Collect(0);
                    break;
                case 1:
                    GC.Collect(1);
                    break;
                case 2:
                    GC.Collect(GC.MaxGeneration);
                    break;
                default:
                    WriteError("Invalid argument passed to GC. Can be 0, 1 or 2", writer, consoleColoring);
                    return false;
            }

            GC.WaitForPendingFinalizers();
            var actionTime = DateTime.UtcNow - startTime;

            ForegroundColor(ConsoleColor.Green, consoleColoring);
            writer.WriteLine("Collected.");
            ResetColor(consoleColoring);
            writer.Write("After collecting, managed memory used:  ");
            ForegroundColor(ConsoleColor.Cyan, consoleColoring);
            writer.Write(new Size(GC.GetTotalMemory(false), SizeUnit.Bytes));
            ResetColor(consoleColoring);
            writer.Write(" at ");
            ForegroundColor(ConsoleColor.Cyan, consoleColoring);
            writer.WriteLine(actionTime.TotalSeconds + " Seconds");
            ResetColor(consoleColoring);
            writer.Flush();

            return true;
        }

        private static bool CommandLog(List<string> args, TextWriter writer, TextReader reader, bool consoleColoring, RavenServer server)
        {
            switch (args.First())
            {
                case "on":
                    LoggingSource.Instance.EnableConsoleLogging();
                    LoggingSource.Instance.SetupLogMode(LogMode.Information, Path.Combine(AppContext.BaseDirectory, server.Configuration.Logs.Path));
                    break;
                case "off":
                    LoggingSource.Instance.DisableConsoleLogging();
                    LoggingSource.Instance.SetupLogMode(LogMode.None, Path.Combine(AppContext.BaseDirectory, server.Configuration.Logs.Path));
                    break;
                case "http-off":
                    RavenServerStartup.SkipHttpLogging = true;
                    goto case "on";
            }

            return true;
        }

        private static bool CommandClear(List<string> args, TextWriter writer, TextReader reader, bool consoleColoring, RavenServer server)
        {
            if (consoleColoring)
                Console.Clear();
            writer.Flush();
            return true;
        }

        private static bool CommandInfo(List<string> args, TextWriter writer, TextReader reader, bool consoleColoring, RavenServer server)
        {
            var memoryInfo = MemoryInformation.GetMemoryInfo();
            ForegroundColor(ConsoleColor.Cyan, consoleColoring);
            writer.WriteLine(" Build {0}, Version {1}, SemVer {2}, Commit {3}\r\n PID {4}, {5} bits, {6} Cores, Arch: {9}\r\n {7} Physical Memory, {8} Available Memory",
                ServerVersion.Build, ServerVersion.Version, ServerVersion.FullVersion, ServerVersion.CommitHash, Process.GetCurrentProcess().Id,
                IntPtr.Size * 8, ProcessorInfo.ProcessorCount, memoryInfo.TotalPhysicalMemory, memoryInfo.AvailableMemory, RuntimeInformation.OSArchitecture);

            var bitsNum = IntPtr.Size * 8;
            if (bitsNum == 64 && server.Configuration.Storage.ForceUsing32BitsPager)
            {
                writer.WriteLine(" Running in 32 bits mode");
            }

            ResetColor(consoleColoring);
            writer.Flush();
            return true;
        }

        private static bool CommandLogo(List<string> args, TextWriter writer, TextReader reader, bool consoleColoring, RavenServer server)
        {
            if (args == null || args.First().Equals("no-clear") == false)
                if (consoleColoring)
                    Console.Clear();
            WelcomeMessage.Print();
            return true;
        }

        private static bool CommandExperimental(List<string> args, TextWriter writer, TextReader reader, bool consoleColoring, RavenServer server)
        {
            var isOn = args.First().Equals("on");
            var isOff = args.First().Equals("off");
            if (!isOff && !isOn)
            {
                WriteError("Experimental cli commands can be set to only on or off. Setting to off.", writer, consoleColoring);
                return false;
            }

            return isOn; // here rc is not an exit code, it is a setter to _experimental
        }

        private static bool CommandLowMem(List<string> args, TextWriter writer, TextReader reader, bool consoleColoring, RavenServer server)
        {
            ResetColor(consoleColoring);
            writer.Write("Before simulating low-mem, memory stats: ");
            ForegroundColor(ConsoleColor.Cyan, consoleColoring);
            var json = MemoryStatsHandler.MemoryStatsInternal();
            var humaneProp = (json["Humane"] as DynamicJsonValue);

            StringBuilder msg = new StringBuilder();
            msg.Append($"Working Set:{humaneProp?["WorkingSet"]}");
            msg.Append($" Unmamanged Memory:{humaneProp?["TotalUnmanagedAllocations"]}");
            msg.Append($" Managed Memory:{humaneProp?["ManagedAllocations"]}");
            writer.WriteLine(msg);
            ResetColor(consoleColoring);
            writer.Write("Sending Low Memory simulation signal... ");
            LowMemoryNotification.Instance.SimulateLowMemoryNotification();
            ForegroundColor(ConsoleColor.Green, consoleColoring);
            writer.WriteLine("Sent.");
            ResetColor(consoleColoring);
            writer.Write("After sending low mem simulation event, memory stats: ");
            ForegroundColor(ConsoleColor.Cyan, consoleColoring);
            msg.Clear();
            msg.Append($"Working Set:{humaneProp?["WorkingSet"]}");
            msg.Append($" Unmamanged Memory:{humaneProp?["TotalUnmanagedAllocations"]}");
            msg.Append($" Managed Memory:{humaneProp?["ManagedAllocations"]}");
            writer.WriteLine(msg);
            ResetColor(consoleColoring);
            writer.Flush();
            return true;
        }


        private static bool CommandHelp(List<string> args, TextWriter writer, TextReader reader, bool consoleColoring, RavenServer server)
        {
            string[][] commandDescription = {
                new[] {"prompt <new prompt>", "Change the cli prompt. Can be used with variables. Type 'helpPrompt` for details"},
                new[] {"helpPrompt", "Detailed prompt command usage"},
                new[] {"stats", "Online server's memory consumption stats, request ratio and documents count"},
                new[] {"resetServer", "Restarts the server (quits and re-run)"},
                new[] {"gc <gen>", "Collect garbage of specified gen (0-2)"},
                new[] {"addServerCert <path> [password]", "Register the certificate as a trusted certificate of a cluster member"},
                new[] {"log <on | off | http-off>", "set log on or off. http-off can be selected to filter log output"},
                new[] {"info", "Print system info and current stats"},
                new[] {"logo [no-clear]", "Clear screen and print initial logo"},
                new[] {"experimental <on | off>", "Set if to allow experimental cli commands"},
                new[] {"quit", "Quit server"},
                new[] {"help", "This help screen"}
            };

            ResetColor(consoleColoring);
            var msg = "RavenDB CLI Help" + Environment.NewLine;
            msg += "================" + Environment.NewLine;
            msg += "Usage: <command> [args] [ && | || <command> [args] ] ..." + Environment.NewLine + Environment.NewLine;
            msg += "Commands:" + Environment.NewLine;
            writer.WriteLine(msg);

            foreach (var cmd in commandDescription)
            {
                ForegroundColor(ConsoleColor.Yellow, consoleColoring);
                writer.Write("\t" + cmd[0]);
                ForegroundColor(ConsoleColor.DarkYellow, consoleColoring);
                writer.WriteLine(new string(' ', 26 - cmd[0].Length) + cmd[1]);
            }
            ResetColor(consoleColoring);
            writer.WriteLine();
            writer.Flush();

            return true;
        }

        private static bool CommandImportDir(List<string> args, TextWriter writer, TextReader reader, bool consoleColoring, RavenServer server)
        {
            // ImportDir <databaseName> <path-to-dir>
            ForegroundColor(ConsoleColor.Yellow, consoleColoring);
            var serverUrl = server.WebUrls[0];
            writer.WriteLine($"ImportDir for database {args[0]} from dir `{args[1]}` to {serverUrl}");
            writer.Flush();

            var port = new Uri(serverUrl).Port;

            var url = $@"http://127.0.0.1:{port}/databases/{args[0]}/smuggler/import-dir?dir={args[1]}";
            using (var client = new HttpClient())
            {
                writer.WriteLine("Sending at " + DateTime.UtcNow);
                writer.Flush();
                var result = client.GetAsync(url).Result;
                writer.WriteLine("At " + DateTime.UtcNow + " : Http Status Code = " + result.StatusCode);
            }
            writer.WriteLine("Http client closed.");
            writer.Flush();
            return true;
        }

        private static bool CommandCreateDb(List<string> args, TextWriter writer, TextReader reader, bool consoleColoring, RavenServer server)
        {
            // CreateDb <databaseName> <DataDir>
            ForegroundColor(ConsoleColor.Yellow, consoleColoring);
            writer.WriteLine($"Create database {args[0]} with DataDir `{args[1]}`");
            writer.Flush();

            var serverUrl = server.WebUrls[0];
            var port = new Uri(serverUrl).Port;

            using (var store = new DocumentStore
            {
                Urls = new[] { $"http://127.0.0.1:{port}" },
                Database = args[0],
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(args[0]);
                doc.Settings["DataDir"] = args[1];
                var res = store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc)).Result;
                writer.WriteLine("Database creation results = " + res.Key);
            }
            writer.Flush();
            return true;
        }

        private readonly Dictionary<Command, SingleAction> _actions = new Dictionary<Command, SingleAction>
        {
            [Command.Prompt] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandPrompt },
            [Command.HelpPrompt] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandHelpPrompt },
            [Command.Stats] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandStats },
            [Command.Gc] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandGc },
            [Command.AddServerCert] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandAddServerCert },
            [Command.Log] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandLog },
            [Command.Clear] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandClear },
            [Command.Info] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandInfo },
            [Command.Logo] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandLogo },
            [Command.Experimental] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandExperimental },
            [Command.LowMem] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandLowMem },
            [Command.ResetServer] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandResetServer },
            [Command.Quit] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandQuit },
            [Command.Help] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandHelp },

            // experimental, will not appear in 'help':
            [Command.ImportDir] = new SingleAction { NumOfArgs = 2, DelegateFync = CommandImportDir, Experimental = true },
            [Command.CreateDb] = new SingleAction { NumOfArgs = 2, DelegateFync = CommandCreateDb, Experimental = true }
        };

        private RavenServer _server;
        private bool _experimental;
        private bool _consoleColoring;

        private enum Command
        {
            // ReSharper disable once UnusedMember.Local
            None,
            Prompt,
            HelpPrompt,
            Quit,
            Log,
            Clear,
            ResetServer,
            Stats,
            Info,
            Gc,
            AddServerCert,
            LowMem,
            Help,
            Logo,
            Experimental,
            ImportDir,
            CreateDb,

            UnknownCommand
        }

        private enum LineState
        {
            // ReSharper disable once UnusedMember.Local
            None,
            Begin,
            AfterCommand,
            AfterArgs,
            Empty
        }

        private enum ConcatAction
        {
            // ReSharper disable once UnusedMember.Local
            None,
            And,
            Or
        }

        private class ParsedCommand
        {
            public Command Command;
            public ConcatAction PrevConcatAction;
            public List<string> Args;
        }

        private class ParsedLine
        {
            public LineState LineState;
            public string ErrorMsg;
            public List<ParsedCommand> ParsedCommands = new List<ParsedCommand>();
        }


        private static void ResetColor(bool consoleColoring)
        {
            if (consoleColoring)
                Console.ResetColor();
        }

        private static void ForegroundColor(ConsoleColor color, bool consoleColoring)
        {
            if (consoleColoring)
                Console.ForegroundColor = color;
        }

        public bool Start(RavenServer server, TextWriter textWriter, TextReader textReader, bool consoleColoring)
        {
            _server = server;

            _writer = textWriter;
            _reader = textReader;
            _consoleColoring = consoleColoring;

            try
            {
                return StartCli();
            }
            catch (Exception ex)
            {
                // incase of cli failure - prevent server from going down, and switch to a (very) simple fallback cli
                _writer.WriteLine("\nERROR in CLI:" + ex);
                _writer.WriteLine("\n\nSwitching to simple cli...");

                while (true)
                {
                    _writer.Write("(simple cli)>");
                    _writer.Flush();
                    var line = ReadLine();
                    if (line == null)
                        continue;
                    switch (line)
                    {
                        case "quit":
                        case "q":
                            return false;
                        case "reset":
                            return true;
                        case "log":
                            LoggingSource.Instance.EnableConsoleLogging();
                            LoggingSource.Instance.SetupLogMode(LogMode.Information, Path.Combine(AppContext.BaseDirectory, _server.Configuration.Logs.Path));
                            break;
                        case "logoff":
                            LoggingSource.Instance.DisableConsoleLogging();
                            LoggingSource.Instance.SetupLogMode(LogMode.None, Path.Combine(AppContext.BaseDirectory, _server.Configuration.Logs.Path));
                            break;
                        case "h":
                        case "help":
                            _writer.WriteLine("Available commands: quit, reset, log, logoff");
                            _writer.Flush();
                            break;
                    }
                }
            }
        }

        public bool StartCli()
        {
            var ctrlCPressed = false;
            if (_consoleColoring)
                Console.CancelKeyPress += (sender, args) =>
                {
                    ctrlCPressed = true;
                };

            while (true)
            {
                PrintCliHeader();
                var line = ReadLine();
                _writer.Flush();

                if (line == null)
                {
                    Thread.Sleep(75); //waiting for Ctrl+C 
                    if (ctrlCPressed)
                        break;
                    _writer.WriteLine("End of standard input detected, switching to server mode...");
                    _writer.Flush();

                    Program.RunAsService();
                    return false;
                }

                var nextline = line;
                var parsedLine = new ParsedLine { LineState = LineState.Begin };

                if (ParseLine(nextline, parsedLine) == false)
                {
                    WriteError(parsedLine.ErrorMsg, _writer, _consoleColoring);
                    continue;
                }

                if (parsedLine.LineState == LineState.Empty)
                    continue;

                var lastRc = true;
                foreach (var parsedCommand in parsedLine.ParsedCommands)
                {
                    if (lastRc == false)
                    {
                        ForegroundColor(WarningColor, _consoleColoring);
                        if (parsedCommand.PrevConcatAction == ConcatAction.And)
                        {
                            _writer.WriteLine($"Warning: Will not execute command `{parsedCommand.Command}` as previous command return non-successful return code");
                            break;
                        }
                        _writer.WriteLine($"Warning: Will execute command `{parsedCommand.Command}` after previous command return non-successful return code");
                    }

                    if (_actions.ContainsKey(parsedCommand.Command) == false)
                    {
                        ForegroundColor(ErrorColor, _consoleColoring);
                        _writer.WriteLine($"CLI Internal Error (missing definition for the command: {parsedCommand.Command})");
                        _writer.WriteLine();
                        lastRc = false;
                        continue;
                    }

                    var cmd = _actions[parsedCommand.Command];

                    try
                    {
                        if (cmd.Experimental)
                        {
                            if (_experimental == false)
                            {
                                ForegroundColor(ErrorColor, _consoleColoring);
                                _writer.WriteLine($"{parsedCommand.Command} is experimental, and can be executed only if expermintal option set to on");
                                _writer.WriteLine();
                                lastRc = false;
                                continue;
                            }
                            ForegroundColor(WarningColor, _consoleColoring);
                            _writer.WriteLine();
                            _writer.Write("Are you sure you want to run experimental command : " + parsedCommand.Command + " ? [y/N] ");
                            _writer.Flush();

                            var k = ReadKey(_consoleColoring, _reader);
                            _writer.Flush();

                            _writer.WriteLine();
                            if (char.ToLower(k).Equals('y') == false)

                            {
                                lastRc = false;
                                continue;
                            }
                        }
                        lastRc = cmd.DelegateFync.Invoke(parsedCommand.Args, _writer, _reader, _consoleColoring, _server);

                        if (parsedCommand.Command == Command.Prompt && lastRc )
                            _promptArgs = parsedCommand.Args;
                        else if (parsedCommand.Command == Command.Experimental)
                        {
                            _experimental = lastRc;
                            lastRc = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        ForegroundColor(ErrorColor, _consoleColoring);
                        _writer.WriteLine(ex);
                        _writer.WriteLine();
                        ResetColor(_consoleColoring);
                        break;
                    }
                    if (lastRc)
                    {
                        _writer.Flush();

                        if (parsedCommand.Command == Command.ResetServer)
                            return true;
                        if (parsedCommand.Command == Command.Quit)
                            return false;
                    }
                }

                if (lastRc == false)
                {
                    ForegroundColor(NonSuccessColor, _consoleColoring);
                    _writer.WriteLine("Command Failed");
                    _writer.WriteLine();
                }
                _writer.Flush();
            }
            _writer.Flush();
            return false; // cannot reach here
        }

        private const ConsoleColor PromptHeaderColor = ConsoleColor.Magenta;
        private const ConsoleColor PromptArrowColor = ConsoleColor.Cyan;
        private const ConsoleColor UserInputColor = ConsoleColor.Green;
        private const ConsoleColor WarningColor = ConsoleColor.Yellow;
        private const ConsoleColor ErrorColor = ConsoleColor.Red;
        private const ConsoleColor NonSuccessColor = ConsoleColor.DarkRed;

        private Command GetCommand(string fromWord)
        {
            if (char.IsNumber(fromWord[0]))
                return Command.UnknownCommand; // TryParse of enum returns true for numbers

            Command cmd = Command.UnknownCommand;
            var txt = fromWord.ToLower();
            Command outText;
            if (Enum.TryParse(fromWord, true, out outText))
                return outText;

            switch (txt)
            {
                case "q":
                    cmd = Command.Quit;
                    break;
                case "h":
                    cmd = Command.Help;
                    break;
                case "cls":
                    cmd = Command.Clear;
                    break;
            }


            return cmd;
        }

        private bool ParseLine(string line, ParsedLine parsedLine, List<string> recursiveWords = null, ConcatAction? lastAction = null)
        {
            List<string> words;
            if (recursiveWords == null)
            {
                words = line.Split(new[] { ',', ' ' },
                    StringSplitOptions.RemoveEmptyEntries).ToList();

                if (words.Count == 0)
                {
                    parsedLine.LineState = LineState.Empty;
                    return true;
                }
            }
            else
            {
                words = recursiveWords;
            }

            if (parsedLine.LineState == LineState.Begin)
            {
                var cmd = GetCommand(words[0]);

                if (cmd == Command.UnknownCommand)
                {
                    parsedLine.ErrorMsg = $"Unknown command: `{words[0]}`";
                    return false;
                }

                ParsedCommand parsedCommand = new ParsedCommand { Command = cmd };
                parsedLine.ParsedCommands.Add(parsedCommand);
                parsedLine.LineState = LineState.AfterCommand;
                words.RemoveAt(0);
                if (lastAction != null)
                {
                    parsedLine.ParsedCommands.Last().PrevConcatAction = lastAction.Value;
                    lastAction = null;
                }
                if (words.Count == 0)
                {
                    if (_actions[parsedLine.ParsedCommands.Last().Command].NumOfArgs > 0)
                    {
                        parsedLine.ErrorMsg = $"Missing argument(s) after command : {parsedLine.ParsedCommands.Last().Command} (should get at least {_actions[parsedLine.ParsedCommands.Last().Command].NumOfArgs} arguments but got none)";
                        return false;
                    }
                    return true;
                }
            }

            if (parsedLine.LineState == LineState.AfterCommand)
            {
                var args = new List<string>();
                int i;
                for (i = 0; i < words.Count; i++)
                {
                    if (i == 0)
                    {
                        if (_actions.ContainsKey(parsedLine.ParsedCommands.Last().Command) == false)
                        {
                            parsedLine.ErrorMsg = $"Internal CLI Error : no definition for `{parsedLine.ParsedCommands.Last().Command}`";
                            return false;
                        }

                        switch (words[0])
                        {
                            case "&&":
                            case "||":
                                if (_actions[parsedLine.ParsedCommands.Last().Command].NumOfArgs != 0)
                                {
                                    parsedLine.ErrorMsg = $"Missing argument(s) after command : {parsedLine.ParsedCommands.Last().Command}";
                                    return false;
                                }
                                break;
                        }
                    }

                    if (words[i] != "&&" && words[i] != "||")
                    {
                        args.Add(words[i]);
                        continue;
                    }

                    if (words[i] == "&&")
                    {
                        parsedLine.LineState = LineState.AfterArgs;
                        lastAction = ConcatAction.And;
                        break;
                    }
                    if (words[i] == "||")
                    {
                        parsedLine.LineState = LineState.AfterArgs;
                        lastAction = ConcatAction.Or;
                        break;
                    }

                    // cannot reach here
                    parsedLine.ErrorMsg = "Internal CLI Error";
                    return false;
                }

                parsedLine.ParsedCommands.Last().Args = args;
                if (lastAction == null)
                {
                    if (args.Count < _actions[parsedLine.ParsedCommands.Last().Command].NumOfArgs)
                    {
                        parsedLine.ErrorMsg = $"Missing argument(s) after command : {parsedLine.ParsedCommands.Last().Command} (should get at least {_actions[parsedLine.ParsedCommands.Last().Command].NumOfArgs} arguments but got {args.Count}";
                        return false;
                    }
                    return true;
                }

                List<string> newWords = new List<string>();
                for (int j = i + 1; j < words.Count; j++)
                {
                    newWords.Add(words[j]);
                }
                parsedLine.LineState = LineState.Begin;
                return ParseLine(null, parsedLine, newWords, lastAction);
            }

            return true;
        }

        private void PrintCliHeader()
        {
            ForegroundColor(PromptHeaderColor, _consoleColoring);
            try
            {
                Prompt.Invoke(_promptArgs, false, _server, _writer);
            }
            catch (Exception ex)
            {
                _writer.WriteLine("PromptError:" + ex.Message);
            }
            ForegroundColor(PromptArrowColor, _consoleColoring);
            _writer.Write("> ");
            ForegroundColor(UserInputColor, _consoleColoring);
            _writer.Flush();
        }

        private static void WriteError(string err, TextWriter writer, bool consoleColoring)
        {
            ForegroundColor(ErrorColor, consoleColoring);
            writer.Write($"ERROR: {err}");
            writer.WriteLine();
            ResetColor(consoleColoring);
            writer.Flush();
        }
    }
}