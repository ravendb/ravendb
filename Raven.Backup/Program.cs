using System;
using NDesk.Options;
using Raven.Abstractions;

namespace Raven.Backup
{
	class Program
	{
		static void Main(string[] args)
		{
			var doReadKeyOnExit = false;
			var op = new BackupOperation { NoWait = false };
			var incrementalBackup = false;
			var optionSet = new OptionSet
			                	{
			                		{"url=", "RavenDB server {0:url}", url => op.ServerUrl = url},
			                		{"dest=", "Full {0:path} to backup folder", path => op.BackupPath = path},
			                		{"nowait", "Return immedialtey without waiting for a response from the server", _ => op.NoWait = true},
			                		{"readkey", "Specifying this flag will make the utility wait for key press before exiting.", _ => doReadKeyOnExit = true},
									{"incremental", "When specified, the backup process will be incremental when done to a folder where a previous backup lies. If dest is an empty folder, or it does not exist, a full backup will be created. For incremental backups to work, the configuration option Raven/Esent/CircularLog must be set to false.", s => incrementalBackup= true}
			                	};

			try
			{
				if (args.Length == 0)
					PrintUsage(optionSet);

				optionSet.Parse(args);
			}
			catch (Exception e)
			{
				Console.WriteLine("Could not understand arguemnts");
				Console.WriteLine(e.Message);
				PrintUsage(optionSet);
				return;
			}

			op.Incremental = incrementalBackup;
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
				return;

			try
			{
				if (op.InitBackup())
					op.WaitForBackup();
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex);
			}

			if (doReadKeyOnExit) Console.ReadKey();
		}

		private static void PrintUsage(OptionSet optionSet)
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
