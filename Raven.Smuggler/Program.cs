//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using NDesk.Options;
using Raven.Abstractions.Data;

namespace Raven.Smuggler
{
	public class Program
	{
		private readonly SmugglerOptions options;
		private readonly OptionSet optionSet;

		private Program()
		{
			options = new SmugglerOptions();
			optionSet = new OptionSet
								{
									{"metadata-filter:{=}", "Filter documents by a metadata property", (key,val) => options.Filters["@metadata." +key] = val},
									{"filter:{=}", "Filter documents by a document property", (key,val) => options.Filters[key] = val},
									{"only-indexes", _ => options.ExportIndexesOnly = true},
									{"include-attachments", s => options.IncludeAttachments = true}
								};
		}

		static void Main(string[] args)
		{
			var program = new Program();
			program.Parse(args);
		}

		private void Parse(string[] args)
		{
			// Do these arguments the traditional way to maintain compatibility
			if (args.Length < 3)
				PrintUsageAndExit(-1);

			if (string.Equals(args[0], "in", StringComparison.OrdinalIgnoreCase))
				options.Action = SmugglerAction.Import;
			else if (string.Equals(args[0], "out", StringComparison.OrdinalIgnoreCase))
				options.Action = SmugglerAction.Export;
			else
				PrintUsageAndExit(-1);

			var url = args[1];
			if (url == null)
				PrintUsageAndExit(-1);
			if (url.EndsWith("/") == false)
				url += "/";
			var connectionStringOptions = new RavenConnectionStringOptions { Url = url };

			options.File = args[2];
			if (options.File == null)
				PrintUsageAndExit(-1);

			try
			{
				optionSet.Parse(args);
			}
			catch (Exception e)
			{
				Console.WriteLine(e.Message);
				PrintUsageAndExit(-1);
			}

			var smugglerApi = new SmugglerApi(connectionStringOptions);

			try
			{
				switch (options.Action)
				{
					case SmugglerAction.Import:
						smugglerApi.ImportData(options);
						break;
					case SmugglerAction.Export:
						smugglerApi.ExportData(options);
						break;
				}
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
				Environment.Exit(-1);
			}
		}

		private void PrintUsageAndExit(int exitCode)
		{
			Console.WriteLine(@"
Smuggler Import/Export utility for RavenDB
----------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------
Usage:
	- Import the dump.raven file to a local instance:
		Raven.Smuggler in http://localhost:8080/ dump.raven
	- Export a local instance to dump.raven:
		Raven.Smuggler out http://localhost:8080/ dump.raven

Command line options:", DateTime.UtcNow.Year);

			optionSet.WriteOptionDescriptions(Console.Out);
			Console.WriteLine();

			Environment.Exit(exitCode);
		}
	}
}