using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using NDesk.Options;

namespace Raven.Backup
{
	class Program
	{
		static void Main(string[] args)
		{
			var op = new BackupOperation { NoWait = false };

			var optionSet = new OptionSet
			            	{
			            		{"url=", "RavenDB server {0:url}", url=>op.ServerUrl = url},
								{"dest=", "Full {0:path} to backup folder", path => op.BackupPath = path},
								{"nowait", "Return immedialtey without waiting for a response from the server", key => op.NoWait = true},
			            	};

			try
			{
				if (args.Length == 0)
					PrintUsage(optionSet);

				optionSet.Parse(args);
			}
			catch (Exception e)
			{
				Console.WriteLine(e.Message);
				PrintUsage(optionSet);
				return;
			}

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
				Console.WriteLine(ex.Message);
			}
		}

		private static void PrintUsage(OptionSet optionSet)
		{
			Console.WriteLine(
				@"
Backup utility for RavenDB
----------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------
Command line ptions:", DateTime.UtcNow.Year);

			optionSet.WriteOptionDescriptions(Console.Out);

			Console.WriteLine();
		}
	}
}
