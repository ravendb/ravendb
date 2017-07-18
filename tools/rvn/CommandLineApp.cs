using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.CommandLineUtils;
using Raven.Server.Config;
using Sparrow.Platform;

namespace rvn
{
    internal static class CommandLineApp
    {
        private const string HelpOptionString = "-h | -? | --help";

        private const string EncryptionCommandsNote =
                "Setup encryption for the server store or decrypt an encrypted store. All commands MUST run under the same user as the one that the RavenDB server is using. The server MUST be offline for the duration of those operations";

        private static CommandLineApplication _app;

        public static int Run(string[] args)
        {
            if (args == null)
                throw new ArgumentNullException(nameof(args));

            _app = new CommandLineApplication();
            _app.Name = "rvn";
            _app.Description =
                @"This utility lets you manage RavenDB offline operations, such as setting encryption mode for the server store. The server store which may contain sensitive information is not encrypted by default (even if it contains encrypted databases). If you want it encrypted, you must do it manually using this tool.";

            _app.HelpOption(HelpOptionString);

            ConfigureOfflineOperationCommand();
            ConfigureAdminChannelCommand();
            
            _app.OnExecute(() =>
            {
                _app.ShowHelp();
                return 1;
            });

            return _app.Execute(args);
        }

        private static void ConfigureAdminChannelCommand()
        {
            _app.Command("admin-channel", cmd =>
            {
                cmd.Description = "Named Pipe Connection to RavenDB with PID. If PID ommited - will try auto pid discovery.";
                cmd.HelpOption(HelpOptionString);
                var pidArg = cmd.Argument("[pid]", "RavenDB Server process ID", cmdWithArg => {});
                cmd.OnExecute(() =>
                {
                    if (int.TryParse(pidArg.Value, out var pid))
                    {
                        AdminChannel.Connect(pid);
                    }
                    else
                    {
                        return ExitWithError("RavenDB server PID argument is mandatory.", cmd);
                    }

                    return 0;
                });
            });
        }

        private static int ExitWithError(string errMsg, CommandLineApplication cmd)
        {
            cmd.ShowHelp();
            cmd.Error.WriteLine(errMsg);
            return 1;
        }

        private static void ConfigureServerCommands() {
            _app.Command("server", cmd => {

                cmd.Description = "Allows to perform a server operation";
                cmd.HelpOption(HelpOptionString);
                
                // TODO @gregolsky
                // WINDOWS only
                // register-service --service-name|-s, 
                // unregister-service --service-name|-s, 
                // start-service --service-name|-s, 
                // stop-service --service-name|-s

            });
        }

        private static void ConfigureOfflineOperationCommand()
        {
            _app.Command("offline-operation", cmd =>
            {
                cmd.Description = "Performs an offline operation on the RavenDB server.";
                cmd.HelpOption(HelpOptionString);

                cmd.Command("init-keys", subcmd =>
                {
                    subcmd.Description = "Generates encryption keys";
                    subcmd.OnExecute(() =>
                    {
                        OfflineOperations.InitKeys();
                        return 0;
                    });
                });
                
                cmd.Command("get-key", subcmd =>
                {
                    subcmd.Description = "exports unprotected server store encryption key to a given directory";
                    subcmd.ExtendedHelpText =
                        "\r\nExports unprotected server store encryption key to RavenDB directory. This key will allow decryption of the server store and must be secured. This is REQUIRED when restoring backups from an encrypted server store.";
                    subcmd.HelpOption(HelpOptionString);

                    subcmd.Argument("[path]", "RavenDB directory path");
                    subcmd.OnExecute(() =>
                    {
                        var path = cmd.Arguments[0].Value;
                        if (string.IsNullOrEmpty(path) == false)
                        {
                            OfflineOperations.GetKey(path);
                        }
                        else
                        {
                            return ExitWithError(
                                "RavenDB directory path argument is mandatory.", subcmd);
                        }

                        return 0;
                    });
                });

                cmd.Command("put-key", subcmd =>
                {
                    subcmd.Description = @"restores and protects the key for current OS user";
                    subcmd.HelpOption(HelpOptionString);
                    subcmd.Argument("[path]", "RavenDB directory path");
                    subcmd.OnExecute(() =>
                    {
                        var path = cmd.Arguments[0].Value;
                        if (string.IsNullOrEmpty(path) == false)
                        {
                            OfflineOperations.PutKey(path);
                        }
                        else
                        {
                            return ExitWithError("RavenDB directory path argument is mandatory.", subcmd);
                        }

                        return 0;
                    });

                    subcmd.ExtendedHelpText =
                        "\r\nRestores the encryption key on the new machine and protects it for the current OS user. This is typically used as part of the restore process of an encrypted server store on a new machine";
                });

                cmd.Command("trust", subcmd =>
                {
                    subcmd.Description = "";

                    subcmd.Argument("[key]", "key");
                    subcmd.Argument("[tag]", "tag");
                    
                    subcmd.OnExecute(() =>
                    {
                        if (subcmd.Arguments.Count == 2)
                        {
                            var keyArg = subcmd.Arguments[0];
                            var tagArg = subcmd.Arguments[1];

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
                    subcmd.Description = "encrypts RavenDB files and saves the key to the same directory";
                    subcmd.ExtendedHelpText = $"\r\nEncrypts RavenDB files and saves the key to a given directory. This key file (secret.key.encrypted) is protected for the current OS user. Once encrypted, The server will only work for the current OS user. It is recommended that you do that as part of the initial setup of the server, before it is running. Encrypted server store can only talk to other encrypted server stores, and only over SSL.\r\n{ EncryptionCommandsNote }";
                    subcmd.HelpOption(HelpOptionString);
                    subcmd.Argument("[path]", "RavenDB directory path");
                    subcmd.OnExecute(() =>
                    {
                        var path = cmd.Arguments[0].Value;
                        if (string.IsNullOrEmpty(path) == false)
                        {
                            OfflineOperations.Encrypt(path);
                        }
                        else
                        {
                            return ExitWithError("RavenDB directory path argument is mandatory.", subcmd);
                        }

                        return 0;
                    });
                });

                cmd.Command("decrypt", subcmd =>
                {
                    subcmd.ExtendedHelpText = $"\r\nDecrypts RavenDB files in a given directory using the key inserted earlier using the put-key command.\r\n{ EncryptionCommandsNote }";
                    subcmd.Description = "descrypts RavenDB files";
                    subcmd.Argument("[path]", "RavenDB directory path");
                    subcmd.OnExecute(() =>
                    {
                        var path = cmd.Arguments[0].Value;
                        if (string.IsNullOrEmpty(path) == false)
                        {
                            OfflineOperations.Decrypt(path);
                        }
                        else
                        {
                            return ExitWithError("RavenDB directory path argument is mandatory.", subcmd);
                        }

                        return 0;
                    });
                });

                cmd.OnExecute(() =>
                {
                    _app.ShowHelp();
                    return 0;
                });
            });
        }
    }
}
