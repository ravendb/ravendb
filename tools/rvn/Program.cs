using System;
using System.Text;

namespace rvn
{
    internal class Program
    {
        private static int Main(string[] args)
        {
            if (args.Length == 0 || args[0].Equals("-h") || args[0].Equals("--help"))
                PrintUsageAndExit();
            if (args[0].Equals("--offline-help"))
                PrintUsageAndExit(true);


            switch (args[0])
            {
                case "admin-channel":
                    if (args.Length > 2)
                        PrintInvalidUsageAndExit($"Invalid number of argumets passed to {args[0]}");
                    var pid = args.Length == 2 ? (int?)Convert.ToInt32(args[1]) : null;
                    AdminChannel.Connect(pid);
                    break;

                case "offline-operation":
                    if (args.Length < 2)
                        PrintInvalidUsageAndExit($"No command after '{args[0]}'");

                    switch (args[1])
                    {
                        case "get-key":
                            if (args.Length != 3)
                                PrintInvalidUsageAndExit($"Invalid number of argumets passed to '{args[0]}' '{args[1]}' command");
                            WriteLine(OfflineOperations.GetKey(args[2]));
                            break;
                        case "put-key":
                            if (args.Length != 3)
                                PrintInvalidUsageAndExit($"Invalid number of argumets passed to '{args[0]}' '{args[1]}' command");
                            WriteLine(OfflineOperations.PutKey(args[2]));
                            break;
                        case "init-keys":
                            if (args.Length != 2)
                                PrintInvalidUsageAndExit($"Invalid number of argumets passed to '{args[0]}' '{args[1]}' command");
                            WriteLine(OfflineOperations.InitKeys());
                            break;
                        case "trust":
                            if (args.Length != 4)
                                PrintInvalidUsageAndExit($"Invalid number of argumets passed to '{args[0]}' '{args[1]}' command");
                            WriteLine(OfflineOperations.Trust(args[2], args[3]));
                            break;
                        case "encrypt":
                            if (args.Length != 3)
                                PrintInvalidUsageAndExit($"Invalid number of argumets passed to '{args[0]}' '{args[1]}' command");
                            WriteLine(OfflineOperations.Encrypt(args[2]));
                            break;
                        case "decrypt":
                            if (args.Length != 3)
                                PrintInvalidUsageAndExit($"Invalid number of argumets passed to '{args[0]}' '{args[1]}' command");
                            WriteLine(OfflineOperations.Decrypt(args[2]));
                            break;
                        default:
                            PrintInvalidUsageAndExit($"Unknown '{args[1]}' command after '{args[0]}'");
                            break;
                    }
                    break;

                default:
                    PrintInvalidUsageAndExit($"Unkown command '{args[0]}' passed to rvn");
                    break;

            }

            return 0;
        }

        private static void WriteLine(string s)
        {
            Console.WriteLine(s);
            Console.Out.Flush();
        }

        private static void PrintUsageAndExit(bool offlineHelp = false)
        {
            var nl = Environment.NewLine;
            var sb = new StringBuilder($"Usage:{nl}");
            sb.Append($"\trvn <command> [options]{nl}{nl}");
            sb.Append($"\tcommand:\tadmin-channel [PID]\tNamed Pipe Connection to RavenDB with PID. If PID ommited - will try auto pid discovery{nl}");
            sb.Append($"\t\t\toffline-operations <operation-command> [args]{nl}{nl}");
            if (offlineHelp == false)
                sb.Append($"\tFor information about offline-operations type : rvn --offline-help{nl}");
            else
                sb.Append(@"
Description: 
    This utility lets you manage RavenDB offline operations, such as setting encryption mode for the server store.

    The server store which may contains sensitive information is not encrypted by default (even if it contains encrypted databases).
    If you want it encrypted, you must do it manually using this tool.
    
Usage:

Setup encryption for the server store or decrypt an encrypted store. All commands MUST run under the same user as the one that
the RavenDB server is using.
The server MUST be offline for the duration of those operations.


    rvn server encrypt <path>

        The encrypt command gets the path of RavenDB's system directory, encrypts the files and saves the key to the same directory.
        This key file (secret.key.encrypted) is protected for the current OS user. Once encrypted, The server will only work for 
        the current user.
        It is recommended that you'll do that as part of the initial setup of the server, before it is running. Encrypted server 
        store can only talk to other encrypted server stores, and only over SSL. 

    rvn server decrypt <path>

        The decrypt command gets the path of RavenDB's system directory on the new machine. It will decrypt the files in that 
        directory using the key which was inserted earlier using the put-key command.


In order to backup the files (for any user) and possibly transfer them to a different machine use the following:

    rvn server get-key <path>

        Once the server store is encrypted run the get-key command with the system directory path, the output it the unprotected key.
        This key will allow decryption of the server store and must be kept safely. This is REQUIRED when restoring backups from an 
        encrypted server store.


    rvn server put-key <path>

        To restore, on a new machine, run the put-key command with the system directory path. This will protect the key on the new machine
        for the current OS user. This is typically used as part of the restore process of an encrypted server store on a new machine.

");
            Console.WriteLine(sb);
            Environment.Exit(1);
        }

        private static void PrintInvalidUsageAndExit(string str)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("ERROR: " + str);
            Console.ResetColor();
            Console.WriteLine("For info type : rvn --help");
            Console.WriteLine();
            Console.Out.Flush();
            Environment.Exit(1);
        }


    }
}

