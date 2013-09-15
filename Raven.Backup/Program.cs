using System;
using NDesk.Options;
using Raven.Abstractions;

namespace Raven.Backup
{
    class Program
    {
        private static bool doReadKeyOnExit;
        private static BackupOperation op;
        private static OptionSet optionSet;

        static void Main(string[] args)
        {
            Initialize();

            ParseArguments(args);

            EnsureMinimalParameters();

            var backupOperationSucceeded = PerformBackup();

            if (doReadKeyOnExit) Console.ReadKey();

            var exitCode = (int)(backupOperationSucceeded ? ExitCodes.Success : ExitCodes.Error);
            Environment.Exit(exitCode);
        }

        private static void Initialize()
        {
            op = new BackupOperation
                 {
                     NoWait = false,
                     Incremental = false
                 };

            optionSet = new OptionSet
			                	{
			                		{"url=", "RavenDB server {0:url}", url => op.ServerUrl = url},
			                		{"dest=", "Full {0:path} to backup folder", path => op.BackupPath = path},
			                		{"nowait", "Return immediately without waiting for a response from the server", _ => op.NoWait = true},
			                		{"readkey", "Specifying this flag will make the utility wait for key press before exiting.", _ => doReadKeyOnExit = true},
									{"incremental", "When specified, the backup process will be incremental when done to a folder where a previous backup lies. If dest is an empty folder, or it does not exist, a full backup will be created. For incremental backups to work, the configuration option Raven/Esent/CircularLog must be set to false.", _ => op.Incremental = true}
			                	};
        }

        private static void ParseArguments(string[] args)
        {
            try
            {
                if (args.Length == 0)
                    PrintUsage();

                optionSet.Parse(args);
            }
            catch (Exception e)
            {
                Console.WriteLine("Could not understand arguments");
                Console.WriteLine(e.Message);
                PrintUsage();

                Environment.Exit((int)ExitCodes.InvalidArguments);
            }
        }

        private static void EnsureMinimalParameters()
        {
            if (string.IsNullOrWhiteSpace(op.ServerUrl))
            {
                Console.WriteLine("Enter RavenDB server URL:");
                op.ServerUrl = Console.ReadLine();
            }

            if (string.IsNullOrWhiteSpace(op.BackupPath))
            {
                Console.WriteLine("Enter backup destination:");
                op.BackupPath = Console.ReadLine();
            }

            if (string.IsNullOrWhiteSpace(op.BackupPath) || string.IsNullOrWhiteSpace(op.ServerUrl))
            {
                Environment.Exit((int)ExitCodes.InvalidArguments);
            }
        }

        private static bool PerformBackup()
        {
            try
            {
                if (op.InitBackup())
                {
                    op.WaitForBackup();
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            return false;
        }

        private static void PrintUsage()
        {
            Console.WriteLine(
                @"
Backup utility for RavenDB
----------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------
Command line options:", SystemTime.UtcNow.Year);

            optionSet.WriteOptionDescriptions(Console.Out);

            Console.WriteLine();
        }
    }
}
