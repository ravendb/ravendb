using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Newtonsoft.Json;
using Raven.Server.Commercial;
using Raven.Server.Commercial.SetupWizard;
using rvn.Parameters;
using Sparrow.Platform;
using Voron.Global;

namespace rvn
{
    internal static class CommandLineApp
    {
        private const string HelpOptionString = "-h | -? | --help";

        private const string EncryptionCommandsNote = "Setup encryption for the server store or decrypt an encrypted store. " +
                                                      "All commands MUST run under the same user as the one that the RavenDB server is using. " +
                                                      "The server MUST be offline for the duration of those operations";

        private static CommandLineApplication _app;

        internal const string OwnCertificate = "own-certificate";
        internal const string LetsEncrypt = "lets-encrypt";

        public static int Run(string[] args)
        {
            if (args == null)
                throw new ArgumentNullException(nameof(args));

            _app = new CommandLineApplication
            {
                Name = "rvn",
                Description = "This utility lets you manage RavenDB offline operations, such as setting encryption mode for the server store. " +
                              "The server store which may contain sensitive information is not encrypted by default (even if it contains encrypted databases). " +
                              "If you want it encrypted, you must do it manually using this tool.",
            };

            _app.HelpOption(HelpOptionString);

            ConfigureOfflineOperationCommand();
            ConfigureAdminChannelCommand();
            ConfigureWindowsServiceCommand();
            ConfigureLogsCommand();
            ConfigureSetupPackage();
            ConfigureInitSetupParams();

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

        private static void ConfigureInitSetupParams()
        {
            _app.Command("init-setup-params", cmd =>
            {
                cmd.Description = "Initializes a skeleton of a RavenDB setup parameters JSON file.";
                
                cmd.HelpOption(HelpOptionString);
                var outputPathOption = ConfigureOutputPathForInitSetupParams(cmd);
                var mode = ConfigureModeOptionForInitSetupParams(cmd);

                cmd.OnExecuteAsync(async token =>
                {
                    string outputFilePath = GetInitSetupParamsOutputPath(outputPathOption, cmd);
                    string setupMode = GetInitSetupParamsSetupMode(mode, cmd);

                    await InitSetupParams.RunAsync(outputFilePath, setupMode, token);
                });
            });
            
        }

        private static string GetInitSetupParamsSetupMode(CommandOption mode, CommandLineApplication app)
        {
            if (mode.HasValue() == false)
                ExitWithError("Output path must have a value.", app);

            var modeValue = mode.Value();
            switch (modeValue)
            {
                case LetsEncrypt:
                case OwnCertificate:
                    break;
                default:
                    ExitWithError($"Unknown setup mode {modeValue}.", app);
                    break;
            }
            
            return modeValue;
        }

        private static string GetInitSetupParamsOutputPath(CommandOption outputPathOption, CommandLineApplication app)
        {
            if (outputPathOption.HasValue() == false)
                ExitWithError("Output path must have a value.", app);

            var outputFilePath = outputPathOption.Value();

            if (File.Exists(outputFilePath))
                ExitWithError($"Output file {outputFilePath} already exists.", app);
            
            return outputFilePath;
        }

        private static void ConfigureSetupPackage()
        {
            _app.Command("create-setup-package", cmd =>
            {
                cmd.Description = "This command creates a RavenDB setup ZIP file";
                cmd.ExtendedHelpText = "Usage example:" +
                                       Environment.NewLine + 
                                       "rvn create-setup-package -m=\"lets-encrypt\" -s=\"json-file-path\" -o=\"output-zip-file-name\"" + Environment.NewLine;
                
                cmd.HelpOption(HelpOptionString);

                var mode = ConfigureModeOption(cmd);
                var setupParam = ConfigureSetupParameters(cmd);
                var packageOutPath = ConfigurePackageOutputFile(cmd);
                var certPath = ConfigureCertPath(cmd);
                var certPass = ConfigureCertPassword(cmd);

                cmd.OnExecuteAsync(async token =>
                {
                    var modeVal = mode.Value();
                    var setupParamVal = setupParam.Value();
                    var packageOutPathVal = packageOutPath.Value();
                    var certPathVal = certPath.Value();
                    var certPassTuple = certPass.Value() ?? Environment.GetEnvironmentVariable("RVN_CERT_PASS");

                    return await CreateSetupPackage(new CreateSetupPackageParameters
                    {
                        SetupJsonPath = setupParamVal,
                        PackageOutputPath = packageOutPathVal,
                        Command = cmd,
                        Mode = modeVal,
                        CertificatePath = certPathVal,
                        CertPassword = certPassTuple,
                        Progress = new SetupProgressAndResult(tuple =>
                        {
                            if (tuple.Message != null)
                            {
                                Console.WriteLine(tuple.Message);
                            }

                            if (tuple.Exception != null)
                            {
                                Console.Error.WriteLine(tuple.Exception.Message);
                            }
                        }),
                        CancellationToken = token
                    });
                });
            });
        }

        private static async Task<int> CreateSetupPackage(CreateSetupPackageParameters parameters)
        {
            byte[] zipFile;
            SetupInfo setupInfo;

            try
            {
                ExtractSetupInfoObjectFromFile(parameters, out setupInfo);
                ValidateSetupInfoAndSetDefaultSetupParametersIfNeeded(setupInfo, parameters);
                ValidateSetupOptions(parameters);
            }
            catch (InvalidOperationException e)
            {
                return ExitWithError(e.Message, parameters.Command);
            }

            switch (parameters.Mode)
            {
                case OwnCertificate:
                {
                    var certBytes = await File.ReadAllBytesAsync(parameters.CertificatePath, parameters.CancellationToken);
                    var certBase64 = Convert.ToBase64String(certBytes);
                    setupInfo.Certificate = certBase64;
                    zipFile = await OwnCertificateSetupUtils.Setup(setupInfo, parameters.Progress, parameters.CancellationToken);
                    break;
                }
                case LetsEncrypt:
                {
                    zipFile = await LetsEncryptSetupUtils.Setup(setupInfo, parameters.Progress, parameters.CancellationToken);
                    break;
                }
                default:
                    return ExitWithError("Invalid mode provided.", parameters.Command);
            }

            try
            {
                await File.WriteAllBytesAsync(parameters.PackageOutputPath, zipFile, parameters.CancellationToken);
            }
            catch (Exception e)
            {
                return ExitWithError($"Failed to write ZIP file to this path: {parameters.PackageOutputPath}\nError: {e}", parameters.Command);
            }

            parameters.Progress.AddInfo($"ZIP file was successfully added to this location: {parameters.PackageOutputPath}");

            return 0;
        }

        private static void ExtractSetupInfoObjectFromFile(CreateSetupPackageParameters parameters, out SetupInfo setupInfo)
        {
            if (string.IsNullOrEmpty(parameters.SetupJsonPath))
            {
                throw new InvalidOperationException("-s|--setup-json-path not provided");
            }

            if (File.Exists(parameters.SetupJsonPath) == false)
            {
                throw new InvalidOperationException($"-s|--setup-json-path path:{parameters.SetupJsonPath} not found");
            }

            try
            {
                using (StreamReader file = File.OpenText(parameters.SetupJsonPath))
                {
                    JsonSerializer serializer = new();
                    setupInfo = (SetupInfo)serializer.Deserialize(file, typeof(SetupInfo));
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to deserialize {nameof(setupInfo)} object from this path {parameters.SetupJsonPath}", ex);
            }
        }

        private static void ValidateSetupInfoAndSetDefaultSetupParametersIfNeeded(SetupInfo setupInfo, CreateSetupPackageParameters parameters)
        {
            var ex = new List<Exception>();
            if (setupInfo.License == null)
            {
                throw new InvalidOperationException($"{nameof(setupInfo.License)} must be set");
            }

            if (setupInfo.License.Keys is null || setupInfo.License.Keys.Any() == false)
            {
                throw new InvalidOperationException($"{nameof(setupInfo.License.Keys)} must be set");
            }

            if (string.IsNullOrEmpty(setupInfo.License.Id.ToString()))
            {
                throw new InvalidOperationException($"{nameof(setupInfo.License.Id)} must be set");
            }

            if (string.IsNullOrEmpty(setupInfo.License.Name))
            {
                throw new InvalidOperationException($"{nameof(setupInfo.License.Name)} must be set");
            }

            if (string.IsNullOrEmpty(setupInfo.Email))
            {
                throw new InvalidOperationException($"{nameof(setupInfo.Email)} must be set");
            }

            if (string.IsNullOrEmpty(setupInfo.Domain))
            {
                throw new InvalidOperationException($"{nameof(setupInfo.Domain)} must be set");
            }

            if (string.IsNullOrEmpty(setupInfo.RootDomain))
            {
                throw new InvalidOperationException($"{nameof(setupInfo.RootDomain)} must be set");
            }

            if (setupInfo.NodeSetupInfos is null || setupInfo.NodeSetupInfos.Any() == false)
            {
                throw new InvalidOperationException($"{nameof(setupInfo.NodeSetupInfos)} must be set");
            }
            
            foreach (var tag in setupInfo.NodeSetupInfos.Keys.Where(tag => IsValidNodeTag(tag) == false))
            {
                ex.Add(new InvalidOperationException($"'{tag}'"));
            }

            if (ex.Count > 0)
                throw new AggregateException($"Node tags must contain only capital letters.Maximum length should be up to 4 characters{Environment.NewLine}Node tags - ",ex);

            foreach (var nodeInfoNode in setupInfo.NodeSetupInfos.Values)
            {
                if (nodeInfoNode?.Addresses is null)
                {
                    throw new InvalidOperationException($"Addresses must be set inside {nameof(setupInfo.NodeSetupInfos)}");
                }

                if (nodeInfoNode.Port == 0)
                {
                    nodeInfoNode.Port = Raven.Client.Constants.Network.DefaultSecuredRavenDbHttpPort;
                }

                if (nodeInfoNode.TcpPort == 0)
                {
                    nodeInfoNode.TcpPort = Raven.Client.Constants.Network.DefaultSecuredRavenDbTcpPort;
                }
            }

            parameters.PackageOutputPath ??= setupInfo.Domain;

            if (string.IsNullOrEmpty(parameters.CertPassword) == false)
            {
                setupInfo.Password = parameters.CertPassword;
            }
            
            if (Path.HasExtension(parameters.PackageOutputPath) == false)
            {
                parameters.PackageOutputPath += ".zip";
            }
            else if (Path.GetExtension(parameters.PackageOutputPath)?.Equals(".zip", StringComparison.OrdinalIgnoreCase) == false)
            {
                throw new InvalidOperationException("--package-output-path file name must end with an extension of .zip");
            }

            parameters.PackageOutputPath = Path.ChangeExtension(parameters.PackageOutputPath, Path.GetExtension(parameters.PackageOutputPath)?.ToLower());
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
                cmd.ExtendedHelpText = cmd.Description =
                    "Open RavenDB CLI session on local machine (using piped name connection). If PID omitted - will try auto pid discovery.";
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
                            subcmd.RemainingArguments.ToList());

                        return 0;
                    });

                    subcmd.UnrecognizedArgumentHandling = UnrecognizedArgumentHandling.StopParsingAndCollect;
                });

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

                    subcmd.ExtendedHelpText = Environment.NewLine +
                                              "Restores the encryption key on a new machine and protects it for the current OS user or the current Master Key (whichever method was chosen to protect secrets). " +
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
                    subcmd.ExtendedHelpText = Environment.NewLine +
                                              "Decrypts RavenDB files in a given directory using the key inserted earlier using the put-key command." +
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
            cmd.Error.WriteLine();
            cmd.Error.WriteLine($"Error: {errMsg}");
            cmd.Error.WriteLine();
            cmd.ShowHelp();
            return 1;
        }

        private static CommandOption ConfigureModeOptionForInitSetupParams(CommandLineApplication cmd)
        {
            var opt = cmd.Option("-m|--mode", "Specify setup mode to use: 'lets-encrypt' or 'own-certificate'", CommandOptionType.SingleValue);
            opt.DefaultValue = "lets-encrypt";
            return opt;
        }
        
        private static CommandOption ConfigureModeOption(CommandLineApplication cmd)
        {
            return cmd.Option("-m|--mode", "Specify setup mode to use: 'lets-encrypt' or 'own-certificate'", CommandOptionType.SingleValue);
        }

        private static CommandOption ConfigureSetupParameters(CommandLineApplication cmd)
        {
            return cmd.Option("-s|--setup-json-path", "Path to JSON file which includes the setup attributes", CommandOptionType.SingleValue);
        }

        private static CommandOption ConfigurePackageOutputFile(CommandLineApplication cmd)
        {
            return cmd.Option("-o|--package-output-path", "Setup package output path (default is $DOMAIN.zip where $DOMAIN comes from setup-json file)", CommandOptionType.SingleValue);
        }
        
        private static CommandOption ConfigureOutputPathForInitSetupParams(CommandLineApplication cmd)
        {
            var opt = cmd.Option("-o|--output-path", "Setup params output path (default: setup.json)", CommandOptionType.SingleValue);
            opt.DefaultValue = "setup.json";
            return opt;
        }

        private static CommandOption ConfigureCertPath(CommandLineApplication cmd)
        {
            return cmd.Option("-c|--cert-path", "Certificate path", CommandOptionType.SingleValue);
        }

        private static CommandOption ConfigureCertPassword(CommandLineApplication cmd)
        {
            return cmd.Option("-p|--password", $"Certificate password{Environment.NewLine}Password can be set from ENV:{Environment.NewLine}Windows - $env:RVN_CERT_PASS=password\nLinux - export RVN_CERT_PASS=password", CommandOptionType.SingleValue);
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

        private static void ValidateRavenDirectory(CommandArgument argument)
        {
            if (string.IsNullOrWhiteSpace(argument.Value))
            {
                throw new InvalidOperationException("RavenDB system directory argument is mandatory.");
            }

            var trimmedFullPath = argument.Value.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            var directory = new DirectoryInfo(trimmedFullPath);

            if (directory.Exists == false)
            {
                throw new InvalidOperationException($"Directory does not exist: {argument.Value}.");
            }

            if (directory.Name.Equals("System"))
            {
                if (File.Exists(Path.Combine(directory.FullName, Constants.DatabaseFilename)) == false)
                    throw new InvalidOperationException("Please provide a valid System directory.");
            }
            else
            {
                if (Directory.Exists(Path.Combine(directory.FullName, "Journals")) == false)
                    throw new InvalidOperationException("Please provide a valid System/Database directory.");
            }
        }

        private static void ValidateSetupOptions(CreateSetupPackageParameters parameters)
        {
            switch (parameters.Mode)
            {
                case OwnCertificate when string.IsNullOrEmpty(parameters.CertificatePath):
                    throw new InvalidOperationException($"-c|--cert-path option must be set when using '{OwnCertificate}' mode.");
                
                case LetsEncrypt when string.IsNullOrEmpty(parameters.CertificatePath) == false:
                    throw new InvalidOperationException($"-c|--cert-path option must be set only when using '{OwnCertificate}' mode.");
                
                case LetsEncrypt when string.IsNullOrEmpty(parameters.CertPassword) == false:
                    throw new InvalidOperationException($"-p|--password option must be set only when using '{OwnCertificate}' mode.");

                case LetsEncrypt: return;

                case OwnCertificate: return;
                
                default: throw new InvalidOperationException($"{parameters.Mode} mode is invalid{Environment.NewLine}-m|--mode option must be set. Please use either '{OwnCertificate}' or '{LetsEncrypt}'");
            }
        }
        
        private static bool IsValidNodeTag(string str)
        {
            return Regex.IsMatch(str, @"^[A-Z]{1,4}$");
        }
        
        private static int PerformOfflineOperation(Func<string> offlineOperation, CommandArgument systemDirArg, CommandLineApplication cmd)
        {
            try
            {
                ValidateRavenDirectory(systemDirArg);
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
