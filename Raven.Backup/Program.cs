using System;
using System.Threading;
using NDesk.Options;
using Raven.Abstractions;
using Raven.Smuggler.Helpers;

namespace Raven.Backup
{
    class Program
    {
        private static bool doReadKeyOnExit;
        private BackupParameters op;
        private BackupOperationDispatcher dispatcher;
        private OptionSet optionSet;

	    static void Main(string[] args)
        {
			var program = new Program();
			program.Initialize();

            program.ParseArguments(args);
            program.EnsureMinimalParameters();

	        var backupOperationSucceeded = program.PerformBackup();

            if (doReadKeyOnExit) 
				Console.ReadKey();

            var exitCode = (int)(backupOperationSucceeded ? ExitCodes.Success : ExitCodes.Error);
            Environment.Exit(exitCode);
        }

        private bool PerformBackup()
        {
            return dispatcher.PerformBackup(op);
        }

        private void Initialize()
        {
	        op = new BackupParameters()
	        {
		        NoWait = false,
		        Incremental = false
	        };

            dispatcher = new BackupOperationDispatcher();
            
			optionSet = new OptionSet();
			optionSet.OnWarning += s => ConsoleHelper.WriteLineWithColor(ConsoleColor.Yellow, s);
			optionSet.Add("url=", OptionCategory.None, "RavenDB server {0:url}", url => op.ServerUrl = url);
			optionSet.Add("dest=", OptionCategory.None, "Full {0:path} to backup folder", path => op.BackupPath = path);
			optionSet.Add("nowait", OptionCategory.None, "Return immediately without waiting for a response from the server", _ => op.NoWait = true);
			optionSet.Add("readkey", OptionCategory.None, "Specifying this flag will make the utility wait for key press before exiting.", _ => doReadKeyOnExit = true);
			optionSet.Add("d|database:", OptionCategory.RestoreDatabase, "The database to operate on. If no specified, the operations will be on the default database unless filesystem option is set.", value => op.Database = value);
			optionSet.Add("f|filesystem:", OptionCategory.RestoreFileSystem, "The filesystem to operate on.", value => op.Filesystem = value);
			optionSet.Add("u|user|username:", OptionCategory.None, "The username to use when the database requires the client to authenticate.", value => op.Credentials.UserName = value);
			optionSet.Add("p|pass|password:", OptionCategory.None, "The password to use when the database requires the client to authenticate.", value => op.Credentials.Password = value);
			optionSet.Add("domain:", OptionCategory.None, "The domain to use when the database requires the client to authenticate.", value => op.Credentials.Domain = value);
			optionSet.Add("key|api-key|apikey:", OptionCategory.None, "The API-key to use, when using OAuth.", value => op.ApiKey = value);
			optionSet.Add("incremental", OptionCategory.None, "When specified, the backup process will be incremental when done to a folder where a previous backup lies. If dest is an empty folder, or it does not exist, a full backup will be created. For incremental backups to work, the configuration option Raven/Esent/CircularLog must be set to false for Esent storage or option Raven/Voron/AllowIncrementalBackups must be set to true for Voron.", _ => op.Incremental = true);
			optionSet.Add("timeout:", OptionCategory.None, "The timeout to use for requests", s => op.Timeout = int.Parse(s));
			optionSet.Add("h|?|help", OptionCategory.Help, string.Empty, v =>
			{
				PrintUsage();
				Environment.Exit(0);
			});
        }

        private void ParseArguments(string[] args)
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

                Console.WriteLine();
                Console.WriteLine(e);

                Environment.Exit((int)ExitCodes.InvalidArguments);
            }
        }

        private void EnsureMinimalParameters()
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

        private void PrintUsage()
        {
            Console.WriteLine(
                @"
Backup utility for RavenDB
----------------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------------
Command line databaseOptions:", SystemTime.UtcNow.Year);

            optionSet.WriteOptionDescriptions(Console.Out);

            Console.WriteLine();
        }
    }
}
