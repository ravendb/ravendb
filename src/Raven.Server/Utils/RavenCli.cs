using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Server.Documents.Handlers.Debugging;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Utils;
using Voron;
using Voron.Impl.Compaction;


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


        private TextWriter _writer;
        private TextReader _reader;
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
            LowMem,
            Help,
            Logo,
            Experimental,
            ImportDir,
            CreateDb,
            Logout,
            
            Encrypt,
            Decrypt,
            InitKeys,
            PutKey,
            GetKey,
            Trust,
 
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
            public readonly List<ParsedCommand> ParsedCommands = new List<ParsedCommand>();
        }

        private List<string> _promptArgs = new List<string> { "ravendb" };

        private const ConsoleColor PromptHeaderColor = ConsoleColor.Magenta;
        private const ConsoleColor PromptArrowColor = ConsoleColor.Cyan;
        private const ConsoleColor UserInputColor = ConsoleColor.Green;
        private const ConsoleColor WarningColor = ConsoleColor.Yellow;
        private const ConsoleColor TextColor = ConsoleColor.White;
        private const ConsoleColor ErrorColor = ConsoleColor.Red;
        private const ConsoleColor SuccessColor = ConsoleColor.Green;

        private class SingleAction
        {
            public int NumOfArgs;
            public Func<List<string>, RavenCli, bool> DelegateFync;
            public bool Experimental { get; set; }
        }

        private static void PrintCliHeader(RavenCli cli)
        {
            if (cli._consoleColoring)
                Console.ForegroundColor = PromptHeaderColor;
            try
            {
                Prompt.Invoke(cli._promptArgs, false, cli._server, cli._writer);
            }
            catch (Exception ex)
            {
                WriteError("PromptError:" + ex.Message, cli);
            }
            WriteText("> ", PromptArrowColor, cli, newLine: false);
            if (cli._consoleColoring)
                Console.ForegroundColor = UserInputColor;
        }

        private static void WriteError(string txt, RavenCli cli)
        {
            WriteText($"ERROR: {txt}", ErrorColor, cli);
            WriteText("", TextColor, cli);
        }

        private static void WriteWarning(string txt, RavenCli cli)
        {
            WriteText($"WARNING: {txt}", WarningColor, cli);
            WriteText("", TextColor, cli);
        }

        private static void WriteText(string txt, ConsoleColor color, RavenCli cli, bool newLine = true)
        {
            if (cli._consoleColoring)
                Console.ForegroundColor = color;
            cli._writer.Write(txt);
            if (newLine)
                cli._writer.WriteLine();
            if (cli._consoleColoring)
                Console.ResetColor();
            cli._writer.Flush();
        }

        private static char ReadKey(RavenCli cli)
        {
            if (cli._consoleColoring)
            {
                var rc = Console.ReadKey().KeyChar;
                cli._writer.Flush();
                return rc;
            }

            cli._writer.Write(CliDelimiter.GetDelimiterString(CliDelimiter.Delimiter.ReadKey));
            cli._writer.Flush();
            var c = cli._reader.Read();
            return Convert.ToChar(c);
        }

        private string ReadLine(RavenCli cli)
        {
            if (cli._consoleColoring == false)
            {
                cli._writer.Write(CliDelimiter.GetDelimiterString(CliDelimiter.Delimiter.ReadLine));
                cli._writer.Flush();
            }

            var rc = _consoleColoring ? Console.ReadLine() : cli._reader.ReadLine();
            cli._writer.Flush();
            return rc;
        }

        private static bool CommandLogout(List<string> args, RavenCli cli)
        {
            if (cli._consoleColoring)
            {
                WriteText("'logout' command not supported on console cli", WarningColor, cli);
                return true;
            }

            cli._writer.WriteLine("Logging out..", TextColor, cli);
            cli._writer.Write(CliDelimiter.GetDelimiterString(CliDelimiter.Delimiter.Logout));
            cli._writer.Flush();
            return true;
        }

        private static bool CommandQuit(List<string> args, RavenCli cli)
        {
            WriteText("", TextColor, cli);
            WriteText("Are you sure you want to quit the server ? [y/N] : ", TextColor, cli, newLine: false);

            var k = ReadKey(cli);
            WriteText("", TextColor, cli);

            return char.ToLower(k).Equals('y');
        }

        private static bool CommandResetServer(List<string> args, RavenCli cli)
        {
            WriteText("", TextColor, cli);
            WriteText("Are you sure you want to reset the server ? [y/N] : ", TextColor, cli, newLine: false);

            var k = ReadKey(cli);
            WriteText("", TextColor, cli);

            return char.ToLower(k).Equals('y');
        }

        private static bool CommandStats(List<string> args, RavenCli cli)
        {
            if (cli._consoleColoring == false)
            {
                // beware not to allow this from remote - will disable local console!                
                WriteText("'stats' command not supported on remote pipe connection. Use `info` or `prompt %M` instead", TextColor, cli);
                return true;
            }

            Console.ResetColor();

            LoggingSource.Instance.DisableConsoleLogging();
            LoggingSource.Instance.SetupLogMode(LogMode.None,
                Path.Combine(AppContext.BaseDirectory, cli._server.Configuration.Logs.Path));

            Program.WriteServerStatsAndWaitForEsc(cli._server);

            return true;
        }

        private static bool CommandPrompt(List<string> args, RavenCli cli)
        {
            try
            {
                Prompt.Invoke(args, true, cli._server, cli._writer);
            }
            catch (Exception ex)
            {
                WriteError("Cannot set prompt to desired args, because of : " + ex.Message, cli);
                return false;
            }
            return true;
        }

        private static bool CommandHelpPrompt(List<string> args, RavenCli cli)
        {
            string[][] commandDescription = {
                new[] {"%D", "UTC Date"},
                new[] {"%T", "UTC Time"},
                new[] {"%M", "Memory information (WS:WorkingSet, UM:Unmanaged, M:Managed, MP:MemoryMapped)"},
                new[] {"%R", "Momentary Req/Sec"},
                new[] {"label", "any label"},
            };

            var msg = new StringBuilder();
            msg.Append("Usage: prompt <[label] | [ %D | %T | %M ] | ...>" + Environment.NewLine + Environment.NewLine);
            msg.Append("Options:" + Environment.NewLine);
            WriteText(msg.ToString(), TextColor, cli);

            foreach (var cmd in commandDescription)
            {
                WriteText("\t" + cmd[0], ConsoleColor.Yellow, cli, newLine: false);
                WriteText(new string(' ', 25 - cmd[0].Length) + cmd[1], ConsoleColor.DarkYellow, cli);
            }
            return true;
        }

        private static bool CommandGc(List<string> args, RavenCli cli)
        {
            int genNum;
            genNum = args == null || args.Count == 0 ? 2 : Convert.ToInt32(args.First());

            WriteText("Before collecting, managed memory used: ", TextColor, cli, newLine: false);
            WriteText(new Size(GC.GetTotalMemory(false), SizeUnit.Bytes).ToString(), ConsoleColor.Cyan, cli);
            var startTime = DateTime.UtcNow;
            WriteText("Garbage Collecting... ", TextColor, cli, newLine: false);

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
                    WriteError("Invalid argument passed to GC. Can be 0, 1 or 2", cli);
                    return false;
            }

            GC.WaitForPendingFinalizers();
            var actionTime = DateTime.UtcNow - startTime;

            WriteText("Collected.", ConsoleColor.Green, cli);
            WriteText("After collecting, managed memory used:  ", TextColor, cli, newLine: false);
            WriteText(new Size(GC.GetTotalMemory(false), SizeUnit.Bytes).ToString(), ConsoleColor.Cyan, cli, newLine: false);
            WriteText(" at ", TextColor, cli, newLine: false);
            WriteText(actionTime.TotalSeconds + " Seconds", ConsoleColor.Cyan, cli);
            return true;
        }

        private static bool CommandLog(List<string> args, RavenCli cli)
        {
            switch (args.First())
            {
                case "on":
                    LoggingSource.Instance.EnableConsoleLogging();
                    LoggingSource.Instance.SetupLogMode(LogMode.Information, Path.Combine(AppContext.BaseDirectory, cli._server.Configuration.Logs.Path));
                    WriteText("Loggin set to ON", ConsoleColor.Green, cli);
                    break;
                case "off":
                    LoggingSource.Instance.DisableConsoleLogging();
                    LoggingSource.Instance.SetupLogMode(LogMode.None, Path.Combine(AppContext.BaseDirectory, cli._server.Configuration.Logs.Path));
                    WriteText("Loggin set to OFF", ConsoleColor.DarkGreen, cli);
                    break;
                case "http-off":
                    WriteText("Setting HTTP logging OFF", ConsoleColor.DarkGreen, cli);
                    RavenServerStartup.SkipHttpLogging = true;
                    goto case "on";
                case "http-on":
                    WriteText("Setting HTTP logging ON", ConsoleColor.Green, cli);
                    RavenServerStartup.SkipHttpLogging = false;
                    goto case "on";
            }

            return true;
        }

        private static bool CommandClear(List<string> args, RavenCli cli)
        {
            if (cli._consoleColoring)
                Console.Clear();
            else
                cli._writer.Write(CliDelimiter.GetDelimiterString(CliDelimiter.Delimiter.Clear));
            cli._writer.Flush();
            return true;
        }

        private static bool CommandInfo(List<string> args, RavenCli cli)
        {
            var memoryInfo = MemoryInformation.GetMemoryInfo();
            WriteText(
                $" Build {ServerVersion.Build}, Version {ServerVersion.Version}, SemVer {ServerVersion.FullVersion}, Commit {ServerVersion.CommitHash}" + Environment.NewLine +
                $" PID {Process.GetCurrentProcess().Id}, {IntPtr.Size * 8} bits, {ProcessorInfo.ProcessorCount} Cores, Arch: {RuntimeInformation.OSArchitecture}" + Environment.NewLine +
                $" {memoryInfo.TotalPhysicalMemory} Physical Memory, {memoryInfo.AvailableMemory} Available Memory",
                ConsoleColor.Cyan, cli);

            var bitsNum = IntPtr.Size * 8;
            if (bitsNum == 64 && cli._server.Configuration.Storage.ForceUsing32BitsPager)
                WriteText(" Running in 32 bits mode", ConsoleColor.DarkCyan, cli);

            return true;
        }

        private static bool CommandLogo(List<string> args, RavenCli cli)
        {
            if (args == null || args.Count == 0 || args.First().Equals("no-clear") == false)
                if (cli._consoleColoring)
                    Console.Clear();
            if (cli._consoleColoring)
                new WelcomeMessage(Console.Out).Print();
            else
                new WelcomeMessage(cli._writer).Print();
            return true;
        }

        private static bool CommandExperimental(List<string> args, RavenCli cli)
        {
            var isOn = args.First().Equals("on");
            var isOff = args.First().Equals("off");
            if (!isOff && !isOn)
            {
                WriteError("Experimental cli commands can be set to only on or off. Setting to off.", cli);
                return false;
            }

            return isOn; // here rc is not an exit code, it is a setter to _experimental
        }

        private static bool CommandLowMem(List<string> args, RavenCli cli)
        {
            WriteText("Before simulating low-mem, memory stats: ", TextColor, cli, newLine: false);

            var json = MemoryStatsHandler.MemoryStatsInternal();
            var humaneProp = (json["Humane"] as DynamicJsonValue);
            StringBuilder msg = new StringBuilder();
            msg.Append($"Working Set:{humaneProp?["WorkingSet"]}");
            msg.Append($" Unmamanged Memory:{humaneProp?["TotalUnmanagedAllocations"]}");
            msg.Append($" Managed Memory:{humaneProp?["ManagedAllocations"]}");
            WriteText(msg.ToString(), ConsoleColor.Cyan, cli);

            WriteText("Sending Low Memory simulation signal... ", TextColor, cli, newLine: false);
            LowMemoryNotification.Instance.SimulateLowMemoryNotification();
            WriteText("Sent.", ConsoleColor.Green, cli);

            WriteText("After sending low mem simulation event, memory stats: ", TextColor, cli, newLine: false);
            msg.Clear();
            msg.Append($"Working Set:{humaneProp?["WorkingSet"]}");
            msg.Append($" Unmamanged Memory:{humaneProp?["TotalUnmanagedAllocations"]}");
            msg.Append($" Managed Memory:{humaneProp?["ManagedAllocations"]}");
            WriteText(msg.ToString(), ConsoleColor.Cyan, cli);

            return true;
        }

        private static bool CommandGetKey(List<string> args, RavenCli cli)
        {
            var srcDir = args[0];

            var masterKey = Sodium.GenerateMasterKey();
            var dstDir = Path.Combine(Path.GetDirectoryName(srcDir), "Temp.Encryption");

            var srcOptions = StorageEnvironmentOptions.ForPath(srcDir);
            var dstOptions = StorageEnvironmentOptions.ForPath(dstDir);

            dstOptions.MasterKey = masterKey;

            var entropy = Sodium.GenerateRandomBuffer(256);
            var protect = SecretProtection.Protect(masterKey, entropy);

            StorageCompaction.Execute(srcOptions, (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)dstOptions);

            using (var f = File.OpenWrite(Path.Combine(dstDir, SecretKeyEncrypted)))
            {
                f.Write(protect, 0, protect.Length);
                f.Write(entropy, 0, entropy.Length);
                f.Flush();
            }

            WriteText($"GetKey: {Path.Combine(dstDir, SecretKeyEncrypted)} Created Successfully", SuccessColor, cli);
            return true;
        }


        private static string RecoverServerStoreKey(string srcDir)
        {
            var keyPath = Path.Combine(srcDir, SecretKeyEncrypted);
            if (File.Exists(keyPath) == false)
                throw new IOException("File not exists:" + keyPath);

            var buffer = File.ReadAllBytes(keyPath);
            var secret = new byte[buffer.Length - 32];
            var entropy = new byte[32];
            Array.Copy(buffer, 0, secret, 0, buffer.Length - 32);
            Array.Copy(buffer, buffer.Length - 32, entropy, 0, 32);

            var key = SecretProtection.Unprotect(secret, entropy);
            return Convert.ToBase64String(key);
        }

        private const string SecretKeyEncrypted = "secret.key.encrypted";

        private static bool CommandPutKey(List<string> args, RavenCli cli)
        {
            var destDir = args[0];

            var base64Key = RecoverServerStoreKey(destDir);
            var entropy = Sodium.GenerateRandomBuffer(256);
            var secret = Convert.FromBase64String(base64Key);
            var protect = SecretProtection.Protect(secret, entropy);

            using (var f = File.OpenWrite(Path.Combine(destDir, SecretKeyEncrypted)))
            {
                f.Write(protect, 0, protect.Length);
                f.Write(entropy, 0, entropy.Length);
                f.Flush();
            }

            WriteText($"PutKey: {Path.Combine(destDir, SecretKeyEncrypted)} Created Successfully", SuccessColor, cli);
            return true;
        }



        private static bool CommandInitKeys(List<string> args, RavenCli cli)
        {
            WriteText("InitKeys is not implemented", ErrorColor, cli);
            return false;
        }

        private static bool CommandEncrypt(List<string> args, RavenCli cli)
        {
            var srcDir = args[0];

            var masterKey = Sodium.GenerateMasterKey();
            var dstDir = Path.Combine(Path.GetDirectoryName(srcDir), "Temp.Encryption");

            var srcOptions = StorageEnvironmentOptions.ForPath(srcDir);
            var dstOptions = StorageEnvironmentOptions.ForPath(dstDir);

            dstOptions.MasterKey = masterKey;

            var entropy = Sodium.GenerateRandomBuffer(256);
            var protect = SecretProtection.Protect(masterKey, entropy);

            StorageCompaction.Execute(srcOptions, (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)dstOptions);

            using (var f = File.OpenWrite(Path.Combine(dstDir, SecretKeyEncrypted)))
            {
                f.Write(protect, 0, protect.Length);
                f.Write(entropy, 0, entropy.Length);
                f.Flush();
            }

            IOExtensions.DeleteDirectory(srcDir);
            Directory.Move(dstDir, srcDir);

            WriteText($"Encrypt: {Path.Combine(dstDir, SecretKeyEncrypted)} Created Successfully", SuccessColor, cli);
            return true;
        }

        private static bool CommandDecrypt(List<string> args, RavenCli cli)
        {
            var srcDir = args[0];

            var dstDir = Path.Combine(Path.GetDirectoryName(srcDir), "Temp.Decryption");
            var bytes = File.ReadAllBytes(Path.Combine(srcDir, SecretKeyEncrypted));
            var secret = new byte[bytes.Length - 32];
            var entropy = new byte[32];
            Array.Copy(bytes, 0, secret, 0, bytes.Length - 32);
            Array.Copy(bytes, bytes.Length - 32, entropy, 0, 32);

            var srcOptions = StorageEnvironmentOptions.ForPath(srcDir);
            var dstOptions = StorageEnvironmentOptions.ForPath(dstDir);

            srcOptions.MasterKey = SecretProtection.Unprotect(secret, entropy);

            StorageCompaction.Execute(srcOptions, (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)dstOptions);

            IOExtensions.DeleteDirectory(srcDir);
            Directory.Move(dstDir, srcDir);

            WriteText($"Decrypt: {Path.Combine(dstDir, SecretKeyEncrypted)} Created Successfully", SuccessColor, cli);
            return true;
        }

        private static bool CommandTrust(List<string> args, RavenCli cli)
        {
            //var key = args[0];
            //var tag = args[1];
            WriteText("InitKeys is not implemented", ErrorColor, cli);
            return false;
        }

        private static bool CommandImportDir(List<string> args, RavenCli cli)
        {
            // ImportDir <databaseName> <path-to-dir>
            var serverUrl = cli._server.WebUrls[0];
            WriteText($"ImportDir for database {args[0]} from dir `{args[1]}` to {serverUrl}", ConsoleColor.Yellow, cli);

            var port = new Uri(serverUrl).Port;

            var url = $@"http://127.0.0.1:{port}/databases/{args[0]}/smuggler/import-dir?dir={args[1]}";
            using (var client = new HttpClient())
            {
                WriteText("Sending at " + DateTime.UtcNow, TextColor, cli);
                var result = client.GetAsync(url).Result;
                WriteText("At " + DateTime.UtcNow + " : Http Status Code = " + result.StatusCode, TextColor, cli);
            }
            WriteText("Http client closed.", TextColor, cli);
            return true;
        }

        private static bool CommandCreateDb(List<string> args, RavenCli cli)
        {
            // CreateDb <databaseName> <DataDir>
            WriteText($"Create database {args[0]} with DataDir `{args[1]}`", ConsoleColor.Yellow, cli);

            var serverUrl = cli._server.WebUrls[0];
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
                WriteText("Database creation results = " + res.Key, TextColor, cli);
            }
            return true;
        }

        private static bool CommandHelp(List<string> args, RavenCli cli)
        {
            string[][] commandDescription = {
                new[] {"prompt <new prompt>", "Change the cli prompt. Can be used with variables. Type 'helpPrompt` for details"},
                new[] {"helpPrompt", "Detailed prompt command usage"},
                new[] {"clear", "Clear screen"},
                new[] {"stats", "Online server's memory consumption stats, request ratio and documents count"},
                new[] {"log [http-]< on | off >", "set log on or off. http-on/off can be selected to filter log output"},
                new[] {"info", "Print system info and current stats"},
                new[] {"logo [no-clear]", "Clear screen and print initial logo"},
                new[] {"gc [gen]", "Collect garbage of specified gen : 0, 1 or default 2"},
                new[] {"lowMem", "Simulate Low-Memory event"},
                new[] {"getKey <source-dir>", "Get server store key from source directory"},
                new[] {"putkey <dest-dir>", "Put server store key from destination directory"},
                new[] {"initKeys", "Print first api key and public key"},
                new[] {"trust <key> <tag>", "Trust key + tag in this server"},
                new[] {"encrypt", "Encrypt server store"},
                new[] {"decrypt", "Decrypt server store"},
                new[] {"experimental <on | off>", "Set if to allow experimental cli commands"},
                new[] {"logout", "Logout (applicable only on piped connection)"},
                new[] {"resetServer", "Restarts the server (quits and re-run)"},
                new[] {"quit", "Quit server"},
                new[] {"help", "This help screen"}
            };

            var msg = new StringBuilder("RavenDB CLI Help" + Environment.NewLine);
            msg.Append("================" + Environment.NewLine);
            msg.Append("Usage: <command> [args] [ && | || <command> [args] ] ..." + Environment.NewLine + Environment.NewLine);
            msg.Append("Commands:" + Environment.NewLine);

            WriteText(msg.ToString(), TextColor, cli);


            foreach (var cmd in commandDescription)
            {
                WriteText("\t" + cmd[0], ConsoleColor.Yellow, cli, newLine: false);
                WriteText(new string(' ', 29 - cmd[0].Length) + cmd[1], ConsoleColor.DarkYellow, cli);
            }
            WriteText("", TextColor, cli);

            return true;
        }

        private readonly Dictionary<Command, SingleAction> _actions = new Dictionary<Command, SingleAction>
        {
            [Command.Prompt] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandPrompt },
            [Command.HelpPrompt] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandHelpPrompt },
            [Command.Stats] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandStats },
            [Command.Gc] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandGc },
            [Command.Log] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandLog },
            [Command.Clear] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandClear },
            [Command.Info] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandInfo },
            [Command.Logo] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandLogo },
            [Command.Experimental] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandExperimental },
            [Command.LowMem] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandLowMem },
            [Command.ResetServer] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandResetServer },
            [Command.Logout] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandLogout },
            [Command.Quit] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandQuit },
            [Command.Help] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandHelp },

            [Command.GetKey] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandGetKey },
            [Command.PutKey] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandPutKey },
            [Command.InitKeys] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandInitKeys },
            [Command.Trust] = new SingleAction { NumOfArgs = 2, DelegateFync = CommandTrust },
            [Command.Encrypt] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandEncrypt },
            [Command.Decrypt] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandDecrypt },

            // experimental, will not appear in 'help':
            [Command.ImportDir] = new SingleAction { NumOfArgs = 2, DelegateFync = CommandImportDir, Experimental = true },
            [Command.CreateDb] = new SingleAction { NumOfArgs = 2, DelegateFync = CommandCreateDb, Experimental = true }
        };

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
                WriteText("\nERROR in CLI:" + ex, ErrorColor, this, newLine: false);
                WriteText("\n\nSwitching to simple cli...", ErrorColor, this, newLine: false);

                while (true)
                {
                    WriteText("(simple cli)>", TextColor, this, newLine: false);
                    var line = ReadLine(this);
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
                            WriteText("Available commands: quit, reset, log, logoff", TextColor, this);
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
            else
            {
                new WelcomeMessage(_writer).Print(); // beware not to print any delimiters until reaching PrintCliHeader
                _writer.WriteLine("Connected to RavenDB Console through named pipe connection..." + Environment.NewLine);
                _writer.Write(CliDelimiter.GetDelimiterString(CliDelimiter.Delimiter.ContinuePrinting));
                _writer.Flush();
            }
            while (true)
            {
                PrintCliHeader(this);
                var line = ReadLine(this);
                _writer.Flush();

                if (line == null)
                {
                    if (_consoleColoring == false)
                    {
                        // for some reason remote pipe couldn't ReadLine
                        WriteText("End of standard input detected. Remote console might not support input", ErrorColor, this);
                        // simulate logout:
                        line = "logout";
                    }
                    else
                    {
                        Thread.Sleep(75); //waiting for Ctrl+C 
                        if (ctrlCPressed)
                            break;
                        WriteText("End of standard input detected, switching to server mode...", WarningColor, this);

                        Program.RunAsService();
                        return false;
                    }
                }

                var parsedLine = new ParsedLine { LineState = LineState.Begin };

                if (ParseLine(line, parsedLine) == false)
                {
                    WriteError(parsedLine.ErrorMsg, this);
                    continue;
                }

                if (parsedLine.LineState == LineState.Empty)
                    continue;

                var lastRc = true;
                foreach (var parsedCommand in parsedLine.ParsedCommands)
                {
                    if (lastRc == false)
                    {
                        if (parsedCommand.PrevConcatAction == ConcatAction.And)
                        {
                            WriteWarning($"Warning: Will not execute command `{parsedCommand.Command}` as previous command return non-successful return code", this);
                            break;
                        }
                        WriteWarning($"Warning: Will execute command `{parsedCommand.Command}` after previous command return non-successful return code", this);
                    }

                    if (_actions.ContainsKey(parsedCommand.Command) == false)
                    {
                        WriteError($"CLI Internal Error (missing definition for the command: {parsedCommand.Command})", this);
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
                                WriteError($"{parsedCommand.Command} is experimental, and can be executed only if expermintal option set to on", this);
                                lastRc = false;
                                continue;
                            }
                            WriteText("", TextColor, this);
                            WriteText("Are you sure you want to run experimental command : " + parsedCommand.Command + " ? [y/N] ", WarningColor, this, newLine: false);

                            var k = ReadKey(this);
                            WriteText("", TextColor, this);


                            if (char.ToLower(k).Equals('y') == false)
                            {
                                lastRc = false;
                                continue;
                            }
                        }
                        lastRc = cmd.DelegateFync.Invoke(parsedCommand.Args, this);

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
                        WriteError(ex.ToString(), this);
                        break;
                    }
                    if (lastRc)
                    {
                        if (parsedCommand.Command == Command.ResetServer)
                        {
                            if (Program.IsRunningAsService || _writer == Console.Out)
                            {
                                if (_consoleColoring == false)
                                {
                                    var str = "Restarting Server";
                                    PrintBothToConsoleAndRemotePipe(str, this, CliDelimiter.Delimiter.RestartServer);
                                }
                                return true;
                            }

                            WriteText("Server is not running as Service. Restart from a remote connection is not allowed." + Environment.NewLine +
                                    "Restart the server from it's main console" + Environment.NewLine, WarningColor, this);
                        }
                        if (parsedCommand.Command == Command.Quit)
                        {
                            if (Program.IsRunningAsService || _writer == Console.Out)
                            {
                                if (_consoleColoring == false)
                                {
                                    var str = "Quitting Server";
                                    PrintBothToConsoleAndRemotePipe(str, this, CliDelimiter.Delimiter.Quit);
                                }
                                return false;
                            }

                            WriteText("Server is not running as Service. Quit from a remote connection is not allowed." + Environment.NewLine +
                                      "Quit the server from it's main console" + Environment.NewLine, WarningColor, this);
                        }
                        if (parsedCommand.Command == Command.Logout)
                        {
                            break;
                        }
                    }
                    else
                    {
                        if (parsedCommand.Command == Command.Quit ||
                            parsedCommand.Command == Command.ResetServer)
                            lastRc = true; // if answered "No" for the above command - don't print ERROR
                    }
                }

                if (lastRc == false)
                {
                    WriteError("Command Failed", this);
                }
            }
            _writer.Flush();
            
            // we are logging out from cli
            Debug.Assert(_consoleColoring == false);
            return false;
        }

        private static void PrintBothToConsoleAndRemotePipe(string str, RavenCli cli, CliDelimiter.Delimiter delimiter)
        {
            cli._writer.WriteLine(str, TextColor, cli);
            cli._writer.Write(CliDelimiter.GetDelimiterString(delimiter));
            cli._writer.Flush();

            Console.ForegroundColor = WarningColor;
            Console.WriteLine();
            Console.WriteLine($"{str} from a remote pipe connection command");
            Console.WriteLine();
            Console.Out.Flush();
        }

        private static Command GetCommand(string fromWord)
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
    }
}