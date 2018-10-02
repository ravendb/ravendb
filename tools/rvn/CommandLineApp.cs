using System;
using System.IO;
using Microsoft.Extensions.CommandLineUtils;
using Sparrow.Platform;

namespace rvn
{
    internal static class CommandLineApp
    {
        private const string HelpOptionString = "-h | -? | --help";

        private const string EncryptionCommandsNote = "Setup encryption for the server store or decrypt an encrypted store. " +
                                                      "All commands MUST run under the same user as the one that the RavenDB server is using. " +
                                                      "The server MUST be offline for the duration of those operations";

        private static CommandLineApplication _app;

        public static int Run(string[] args)
        {
            if (args == null)
                throw new ArgumentNullException(nameof(args));

            _app = new CommandLineApplication
            {
                Name = "rvn",
                Description = "This utility lets you manage RavenDB offline operations, such as setting encryption mode for the server store. " +
                              "The server store which may contain sensitive information is not encrypted by default (even if it contains encrypted databases). " +
                              "If you want it encrypted, you must do it manually using this tool."
            };

            _app.HelpOption(HelpOptionString);

            ConfigureOfflineOperationCommand();
            ConfigureAdminChannelCommand();
            ConfigureWindowsServiceCommand();
            ConfigureLogsCommand();

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

        private static void ConfigureLogsCommand()
        {
            _app.Command("logstream", cmd =>
            {
                cmd.ExtendedHelpText = cmd.Description = "Tail server logs.";
                cmd.HelpOption(HelpOptionString);

                var pidArg = cmd.Argument("ProcessID", "RavenDB Server process ID");

                cmd.OnExecute(() =>
                {
                    LogStream logStream;
                    if (string.IsNullOrEmpty(pidArg.Value))
                    {
                        logStream = new LogStream();
                    }
                    else
                    {
                        if (int.TryParse(pidArg.Value, out var pid))
                        {
                            logStream = new LogStream(pid);
                        }
                        else
                        {
                            return ExitWithError("RavenDB server PID argument is invalid.", cmd);
                        }
                    }

                    Console.CancelKeyPress += (sender, args) => logStream?.Stop();

                    using (logStream)
                        logStream.Connect().Wait();

                    return 0;
                });
            });
        }

        private static void ConfigureAdminChannelCommand()
        {
            _app.Command("admin-channel", cmd =>
            {
                cmd.ExtendedHelpText = cmd.Description = "Open RavenDB CLI session on local machine (using piped name connection). If PID omitted - will try auto pid discovery.";
                cmd.HelpOption(HelpOptionString);
                var pidArg = cmd.Argument("ProcessID", "RavenDB Server process ID");
                cmd.OnExecute(() =>
                {
                    if (string.IsNullOrEmpty(pidArg.Value))
                    {
                        AdminChannel.Connect(null);
                    }
                    else
                    {
                        if (int.TryParse(pidArg.Value, out var pid))
                        {
                            AdminChannel.Connect(pid);
                        }
                        else
                        {
                            return ExitWithError("RavenDB server PID argument is invalid.", cmd);
                        }
                    }

                    return 0;
                });
            });
        }

        private static void ConfigureWindowsServiceCommand()
        {
            const string defaultServiceName = "RavenDB";

            if (PlatformDetails.RunningOnPosix)
            {
                return;
            }

            _app.Command("windows-service", cmd =>
            {
                var serviceString = PlatformDetails.RunningOnPosix ? "daemon" : "Windows Service";
                cmd.Description = $"Allows to perform an operation on RavenDB Server which is running as {serviceString}";
                cmd.HelpOption(HelpOptionString);
                ConfigureServiceNameOption(cmd);

                cmd.Command("register", subcmd =>
                {
                    var serviceNameOpt = ConfigureServiceNameOption(subcmd);
                    var serviceUserNameOpt = ConfigureServiceUserNameOption(subcmd);
                    var serviceUserPasswordOpt = ConfigureServiceUserPasswordOption(subcmd);
                    var serverDirOpt = ConfigureServerDirOption(subcmd);

                    subcmd.Description = "Registers RavenDB Server as Windows Service";
                    subcmd.ExtendedHelpText = Environment.NewLine + "Registers RavenDB Server as Windows Service. " +
                                              "Any additional arguments passed after command options are going to be passed to the server.";
                    subcmd.HelpOption(HelpOptionString);

                    subcmd.OnExecute(() =>
                    {
                        WindowsService.Register(
                            serviceNameOpt.Value() ?? defaultServiceName,
                            serviceUserNameOpt.Value(),
                            serviceUserPasswordOpt.Value(),
                            serverDirOpt.Value(),
                            subcmd.RemainingArguments);

                        return 0;
                    });

                }, throwOnUnexpectedArg: false);

                cmd.Command("unregister", subcmd =>
                {
                    var serviceNameOpt = ConfigureServiceNameOption(subcmd);
                    subcmd.ExtendedHelpText = subcmd.Description = "Unregisters RavenDB Server Windows Service";
                    subcmd.HelpOption(HelpOptionString);

                    subcmd.OnExecute(() =>
                    {
                        WindowsService.Unregister(serviceNameOpt.Value() ?? defaultServiceName);
                        return 0;
                    });
                });

                cmd.Command("start", subcmd =>
                {
                    var serviceNameOpt = ConfigureServiceNameOption(subcmd);
                    subcmd.Description = "Starts RavenDB Server Windows Service";
                    subcmd.HelpOption(HelpOptionString);

                    subcmd.OnExecute(() =>
                    {
                        WindowsService.Start(serviceNameOpt.Value() ?? defaultServiceName);
                        return 0;
                    });
                });

                cmd.Command("stop", subcmd =>
                {
                    var serviceNameOpt = ConfigureServiceNameOption(subcmd);
                    subcmd.ExtendedHelpText = subcmd.Description = "Stops RavenDB Server Windows Service";
                    subcmd.HelpOption(HelpOptionString);

                    subcmd.OnExecute(() =>
                    {
                        WindowsService.Stop(serviceNameOpt.Value() ?? defaultServiceName);
                        return 0;
                    });
                });

                cmd.OnExecute(() =>
                {
                    cmd.ShowHelp();
                    return 1;
                });
            });
        }

        private static void ConfigureOfflineOperationCommand()
        {
            _app.Command("offline-operation", cmd =>
            {
                const string systemDirArgText = "[RavenDB system directory]";
                const string systemDirArgDescText = "RavenDB system directory";

                cmd.Description = "Performs an offline operation on the RavenDB Server.";
                cmd.HelpOption(HelpOptionString);

                cmd.Command("init-keys", subcmd =>
                {
                    subcmd.ExtendedHelpText = subcmd.Description = "Initializes keys";
                    subcmd.HelpOption(HelpOptionString);
                    subcmd.OnExecute(() =>
                    {
                        var result = OfflineOperations.InitKeys();
                        Console.WriteLine(result);
                        return 0;
                    });
                });

                cmd.Command("get-key", subcmd =>
                {
                    subcmd.Description = "Exports unprotected server store encryption key";
                    subcmd.ExtendedHelpText = Environment.NewLine + "Exports the unprotected server store encryption key. " +
                                              "This key will allow decryption of the server store and must be kept safely. " +
                                              "This is REQUIRED when restoring backups from an encrypted server store.";
                    subcmd.HelpOption(HelpOptionString);

                    subcmd.Argument(systemDirArgText, systemDirArgDescText, systemDir =>
                    {
                        subcmd.OnExecute(() =>
                        {
                            return PerformOfflineOperation(
                                () => OfflineOperations.GetKey(systemDir.Value), systemDir, subcmd);
                        });
                    });
                });

                cmd.Command("put-key", subcmd =>
                {
                    subcmd.Description = @"Restores and protects the key for current OS user";
                    subcmd.HelpOption(HelpOptionString);
                    subcmd.Argument(systemDirArgText, systemDirArgDescText, args =>
                    {
                        subcmd.OnExecute(() =>
                        {
                            if (args.Values.Count != 2)
                                return ExitWithError("Usage: ./rvn offline-operation put-key <path-to-system-dir> <key>", cmd);

                            return PerformOfflineOperation(
                                () => OfflineOperations.PutKey(args.Values[0], args.Values[1]), args, subcmd);
                        });
                    }, multipleValues: true);

                    subcmd.ExtendedHelpText = Environment.NewLine + "Restores the encryption key on a new machine and protects it for the current OS user or the current Master Key (whichever method was chosen to protect secrets). " +
                                              "This is typically used as part of the restore process of an encrypted server store on a new machine";
                });

                cmd.Command("trust", subcmd =>
                {
                    subcmd.Description = string.Empty;
                    subcmd.HelpOption(HelpOptionString);

                    var keyArg = subcmd.Argument("Key", "The key");
                    var tagArg = subcmd.Argument("Tag", "The tag");

                    subcmd.OnExecute(() =>
                    {
                        if (subcmd.Arguments.Count == 2)
                        {
                            OfflineOperations.Trust(keyArg.Value, tagArg.Value);
                        }
                        else
                        {
                            return ExitWithError("Key and tag arguments are mandatory.", subcmd);
                        }

                        return 0;
                    });
                });

                cmd.Command("encrypt", subcmd =>
                {
                    subcmd.Description = "Encrypts RavenDB files and saves the key to the same directory";
                    subcmd.ExtendedHelpText = Environment.NewLine + "Encrypts RavenDB files and saves the key to a given directory. " +
                                              "Once encrypted, the server will only work for the current OS user or the current Master Key (whichever method was chosen to protect secrets)" +
                                              "It is recommended to do this at the very start, as part of the initial cluster setup, right after the server was launched for the first time." +
                                              "Encrypted server stores can only talk to other encrypted server stores, and only over SSL." +
                                              Environment.NewLine + EncryptionCommandsNote;

                    subcmd.HelpOption(HelpOptionString);
                    subcmd.Argument(systemDirArgText, systemDirArgDescText, systemDir =>
                    {
                        subcmd.OnExecute(() =>
                        {
                            return PerformOfflineOperation(
                                () => OfflineOperations.Encrypt(systemDir.Value), systemDir, subcmd);
                        });
                    });
                });

                cmd.Command("decrypt", subcmd =>
                {
                    subcmd.ExtendedHelpText = Environment.NewLine + "Decrypts RavenDB files in a given directory using the key inserted earlier using the put-key command." +
                                              Environment.NewLine + EncryptionCommandsNote;
                    subcmd.HelpOption(HelpOptionString);
                    subcmd.Description = "Decrypts RavenDB files";
                    subcmd.Argument(systemDirArgText, systemDirArgDescText, systemDir =>
                    {
                        subcmd.OnExecute(() =>
                        {
                            return PerformOfflineOperation(
                                () => OfflineOperations.Decrypt(systemDir.Value), systemDir, subcmd);
                        });
                    });
                });

                cmd.OnExecute(() =>
                {
                    cmd.ShowHelp();
                    return 1;
                });
            });
        }

        private static int ExitWithError(string errMsg, CommandLineApplication cmd)
        {
            cmd.Error.WriteLine(errMsg);
            cmd.ShowHelp();
            return 1;
        }

        private static CommandOption ConfigureServiceNameOption(CommandLineApplication cmd)
        {
            return cmd.Option("--service-name", "RavenDB Server Windows Service name", CommandOptionType.SingleValue);
        }

        private static CommandOption ConfigureServiceUserNameOption(CommandLineApplication cmd)
        {
            return cmd.Option("--service-user-name", "RavenDB Server Windows Service user name", CommandOptionType.SingleValue);
        }

        private static CommandOption ConfigureServiceUserPasswordOption(CommandLineApplication cmd)
        {
            return cmd.Option("--service-user-password", "RavenDB Server Windows Service user password", CommandOptionType.SingleValue);
        }

        private static CommandOption ConfigureServerDirOption(CommandLineApplication cmd)
        {
            return cmd.Option("--server-dir", "RavenDB Server directory", CommandOptionType.SingleValue);
        }

        private static void ValidateRavenSystemDir(CommandArgument systemDirArg)
        {
            var fullPath = systemDirArg.Value.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                           + Path.DirectorySeparatorChar;
            if (string.IsNullOrEmpty(systemDirArg.Value))
            {
                throw new InvalidOperationException("RavenDB system directory argument is mandatory.");
            }

            if (Directory.Exists(systemDirArg.Value) == false)
            {
                throw new InvalidOperationException($"Directory does not exist: { systemDirArg.Value }.");
            }

            if (fullPath.EndsWith("System\\"))
            {
                if (File.Exists(fullPath + "Raven.voron") == false)
                    throw new InvalidOperationException("Please provide a valid System directory.");
            }
            else
            {
                if (Directory.Exists(fullPath + "Journals") == false)
                    throw new InvalidOperationException("Please provide a valid System/Database directory.");
            }
        }

        private static int PerformOfflineOperation(Func<string> offlineOperation, CommandArgument systemDirArg, CommandLineApplication cmd)
        {
            try
            {
                ValidateRavenSystemDir(systemDirArg);
                var result = offlineOperation();
                cmd.Out.WriteLine(result);
                return 0;
            }
            catch (InvalidOperationException e)
            {
                return ExitWithError(e.Message, cmd);
            }
        }
    }
}
