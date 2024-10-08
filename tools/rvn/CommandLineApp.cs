using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Commercial;
using Sparrow.Json;
using Sparrow.Platform;
using Voron.Global;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

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
        internal const string Unsecured = "unsecured";

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
            ConfigurePutClientCertificateCommand();

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
                case Unsecured:
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
                                       "rvn create-setup-package -m=\"lets-encrypt\" -s=\"json-file-path\" -o=\"output-zip-file-name\" --generate-helm-values[=\"values.yaml\"]" +
                                       Environment.NewLine;
                
                cmd.HelpOption(HelpOptionString);

                var mode = ConfigureModeOption(cmd);
                var setupParam = ConfigureSetupParameters(cmd);
                var packageOutPath = ConfigurePackageOutputFile(cmd);
                var certPath = ConfigureCertPath(cmd);
                var certPass = ConfigureCertPassword(cmd);
                var generateHelmValues = ConfigureGenerateValues(cmd);
                var acmeUrl = ConfigureAcmeUrl(cmd);

                cmd.OnExecuteAsync(async token =>
                {
                    var modeVal = mode.Value();
                    var setupParamVal = setupParam.Value();
                    var packageOutPathVal = packageOutPath.Value();
                    var certPathVal = certPath.Value();
                    var certPassTuple = certPass.Value() ?? Environment.GetEnvironmentVariable("RVN_CERT_PASS");
                    var generateHelmValuesVal = generateHelmValues.HasValue() ? generateHelmValues.Value() is null ? "values.yaml": generateHelmValues.Value() : null;
                    var acmeUrlVal = acmeUrl.Value();

                    return await CreateSetupPackage(new CreateSetupPackageParameters
                    {
                        SetupJsonPath = setupParamVal,
                        PackageOutputPath = packageOutPathVal,
                        Command = cmd,
                        Mode = modeVal,
                        CertificatePath = certPathVal,
                        CertPassword = certPassTuple,
                        AcmeUrl = acmeUrlVal,
                        HelmValuesOutputPath = generateHelmValuesVal,
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
                        RegisterTcpDnsRecords = generateHelmValuesVal is not null,
                        CancellationToken = token
                    });
                });
            });
        }

        private static async Task<int> CreateSetupPackage(CreateSetupPackageParameters parameters)
        {
            SetupInfoBase setupInfoBase;
            try
            {
                ValidateSetupOptions(parameters);
                ExtractSetupInfoObjectFromFile(parameters, out setupInfoBase);
                setupInfoBase.ValidateInfo(parameters);
                
                switch (setupInfoBase)
                {
                    case UnsecuredSetupInfo unsecuredSetupInfo:
                        parameters.UnsecuredSetupInfo = unsecuredSetupInfo;
                        break;
                    case SetupInfo setupInfo:
                        parameters.SetupInfo = setupInfo;
                        break;
                    default:
                        throw new NotSupportedException($"{setupInfoBase.GetType()} is not supported");
                }

            }
            catch (InvalidOperationException e)
            {
                return ExitWithError(e.Message, parameters.Command);
            }

            try
            {
                var zipFile = await setupInfoBase.GenerateZipFile(parameters);
                await File.WriteAllBytesAsync(parameters.PackageOutputPath, zipFile, parameters.CancellationToken);
            }
            catch (Exception e)
            {
                return ExitWithError($"Failed to write ZIP file to this path: {parameters.PackageOutputPath}\nError: {e}", parameters.Command);
            }

            parameters.Progress.AddInfo($"ZIP file was successfully added to this location: {parameters.PackageOutputPath}");

            if (parameters.HelmValuesOutputPath is null) return 0;

            string extractedValues;
            
            try
            {
                ValidateHelmValuesPath(parameters);
                extractedValues = GenerateHelmValues(parameters);
            }
            catch (Exception e)
            {
                return ExitWithError($"Failed to create helm values : {parameters.HelmValuesOutputPath} file. Error: {e}", parameters.Command);
            }

            try
            {
                await File.WriteAllTextAsync(parameters.HelmValuesOutputPath, extractedValues,parameters.CancellationToken);
            }
            catch (Exception e)
            {
                return ExitWithError($"Failed to write YAML file to this path: {parameters.HelmValuesOutputPath}\nError: {e}", parameters.Command);
            }
            
            parameters.Progress.AddInfo($"YAML file was successfully added to this location: {parameters.HelmValuesOutputPath}");
            return 0;
        }

        private static void ExtractSetupInfoObjectFromFile(CreateSetupPackageParameters parameters, out SetupInfoBase setupInfoBase)
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
                    setupInfoBase = (SetupInfoBase)serializer.Deserialize(file, parameters.Mode.Equals(Unsecured)  ? typeof(UnsecuredSetupInfo) : typeof(SetupInfo));
                }  
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to deserialize {nameof(setupInfoBase)} object from this path {parameters.SetupJsonPath}", ex);
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

        private static void ConfigurePutClientCertificateCommand()
        {
            _app.Command("put-client-certificate", cmd =>
            {
                cmd.ExtendedHelpText = cmd.Description = "Register certificate as the valid client certificate for the RavenDB server.";
                cmd.HelpOption(HelpOptionString);
                var ravenServerUrlArg= cmd.Argument("ServerUrl", "RavenDB server url");
                var serverCertificatePathArg = cmd.Argument("ServerCertificateFilePath", "Server PFX certificate path");
                var clientCertificatePathArg = cmd.Argument("ClientCertificateFilePath", "Client PFX certificate path");
                
                cmd.OnExecute(() =>
                {
                    if (string.IsNullOrWhiteSpace(serverCertificatePathArg.Value))
                    {
                        return ExitWithError("Server certificate file path is invalid.", cmd);
                    }
                    if (string.IsNullOrWhiteSpace(clientCertificatePathArg.Value))
                    {
                        return ExitWithError("Client certificate file path is invalid.", cmd);
                    }

                    
                    X509Certificate2 clientCertificate = X509CertificateLoader.LoadCertificateFromFile(clientCertificatePathArg.Value);
                    X509Certificate2 serverCertificate = X509CertificateLoader.LoadCertificateFromFile(serverCertificatePathArg.Value);
                    var name = Path.GetFileNameWithoutExtension(clientCertificatePathArg.Value);
                    try
                    {
                        DocumentStore store = new() {Certificate = serverCertificate, Urls = new[] {ravenServerUrlArg.Value}};
                        store.Initialize();
                        var operation = new PutClientCertificateOperation(name, clientCertificate, new Dictionary<string,DatabaseAccess>(), SecurityClearance.ClusterAdmin);
                        store.Maintenance.Server.Send(operation);
                    }
                    catch (Exception e)
                    {
                        return ExitWithError($"Failed to put client certificate to the RavenDB server under the address: {ravenServerUrlArg.Value}{Environment.NewLine}{e}", cmd);
                    }
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
            var opt = cmd.Option("-m|--mode", "Specify setup mode to use: 'lets-encrypt', 'own-certificate' or 'unsecured'", CommandOptionType.SingleValue);
            opt.DefaultValue = "lets-encrypt";
            return opt;
        }
        
        private static CommandOption ConfigureAcmeUrl(CommandLineApplication cmd)
        {
            var opt = cmd.Option("--acme-url", "Specify acme url to use (default: 'https://acme-v02.api.letsencrypt.org/directory')", CommandOptionType.SingleValue);
            opt.DefaultValue = "https://acme-v02.api.letsencrypt.org/directory";
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

        private static CommandOption ConfigureGenerateValues(CommandLineApplication cmd)
        {
            var opt = cmd.Option("--generate-helm-values", "Path to values.yaml", CommandOptionType.SingleOrNoValue);
            return opt;
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

                case Unsecured: return;
                
                case OwnCertificate: return;
                
                default: throw new InvalidOperationException($"{parameters.Mode} mode is invalid. -m|--mode option must be set.{Environment.NewLine}Please use either '{Unsecured}', '{OwnCertificate}' and '{LetsEncrypt}'");
            }
        }


        private static void ValidateHelmValuesPath(CreateSetupPackageParameters parameters)
        {
            if (string.IsNullOrWhiteSpace(parameters.HelmValuesOutputPath))
            {
                throw new InvalidOperationException("Please provide a valid file name for the helm values yaml.");
            }
            
            if (Path.HasExtension(parameters.HelmValuesOutputPath) == false)
            {
                parameters.HelmValuesOutputPath += ".yaml";
            }
            else if (Path.GetExtension(parameters.HelmValuesOutputPath)?.Equals(".yaml", StringComparison.OrdinalIgnoreCase) == false)
            {
                throw new InvalidOperationException("--generate-helm-values file name must end with an extension of .yaml");
            }
        }

        private static string GenerateHelmValues(CreateSetupPackageParameters parameters)
        {
            using var context = JsonOperationContext.ShortTermSingleUse();
            var jsonBlittable = context.ReadObject(parameters.SetupInfo.License.ToJson(), "license");
            HelmInfo helmInfo = new()
            {
                Domain = $"{parameters.SetupInfo.Domain}.{parameters.SetupInfo.RootDomain}",
                Email = parameters.SetupInfo.Email,
                License = jsonBlittable.ToString(),
                NodeTags = parameters.SetupInfo.NodeSetupInfos.Keys.ToList(),
                SetupMode = parameters.Mode == "lets-encrypt" ? "LetsEncrypt" : "Secured"
            };
            
            var serializer = new SerializerBuilder().WithNamingConvention(CamelCaseNamingConvention.Instance).Build();
            var yaml = serializer.Serialize(helmInfo);
            return yaml;
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
