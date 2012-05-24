//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Net;
using NDesk.Options;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;

namespace Raven.Smuggler
{
	public class Program
	{
		private readonly RavenConnectionStringOptions connectionStringOptions;
		private readonly SmugglerOptions options;
		private readonly OptionSet optionSet;
		bool incremental;

		private Program()
		{
			connectionStringOptions = new RavenConnectionStringOptions();
			options = new SmugglerOptions();

			optionSet = new OptionSet
			            	{
			            		{
			            			"operate-on-types:", "Specify the types to operate on. Specify the types to operate on. You can specify more than one type by combining items with a comma." + Environment.NewLine +
			            			                     "Default is all items." + Environment.NewLine +
			            			                     "Usage example: Indexes,Documents,Attachments", value =>
			            			                                                                     	{
			            			                                                                     		try
			            			                                                                     		{
			            			                                                                     			options.OperateOnTypes = (ItemType) Enum.Parse(typeof (ItemType), value);
			            			                                                                     		}
			            			                                                                     		catch (Exception e)
			            			                                                                     		{
			            			                                                                     			PrintUsageAndExit(e);
			            			                                                                     		}
			            			                                                                     	}
			            			},
			            		{
			            			"metadata-filter:{=}", "Filter documents by a metadata property." + Environment.NewLine +
			            			                       "Usage example: Raven-Entity-Name=Posts", (key, val) => options.Filters["@metadata." + key] = val
			            			},
			            		{
			            			"filter:{=}", "Filter documents by a document property" + Environment.NewLine +
			            			              "Usage example: Property-Name=Value", (key, val) => options.Filters[key] = val
			            			},
			            		{"d|database:", "The database to operate on. If no specified, the operations will be on the default database.", value => connectionStringOptions.DefaultDatabase = value},
			            		{"u|user|username:", "The username to use when the database requires the client to authenticate.", value => Credentials.UserName = value},
			            		{"p|pass|password:", "The password to use when the database requires the client to authenticate.", value => Credentials.Password = value},
			            		{"domain:", "The domain to use when the database requires the client to authenticate.", value => Credentials.Domain = value},
			            		{"key|api-key:", "The API-key to use, when using OAuth.", value => connectionStringOptions.ApiKey = value},
								{"incremental", "States usage of incremental operations", _ => incremental = true },
			            		{"h|?|help", v => PrintUsageAndExit(0)},
			            	};
		}

		private NetworkCredential Credentials
		{
			get { return connectionStringOptions.Credentials ?? (connectionStringOptions.Credentials = new NetworkCredential()); }
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

			SmugglerAction action = SmugglerAction.Export;
			if (string.Equals(args[0], "in", StringComparison.OrdinalIgnoreCase))
				action = SmugglerAction.Import;
			else if (string.Equals(args[0], "out", StringComparison.OrdinalIgnoreCase))
				action = SmugglerAction.Export;
			else
				PrintUsageAndExit(-1);

			var url = args[1];
			if (url == null)
			{
				PrintUsageAndExit(-1);
				return;
			}
			connectionStringOptions.Url = url;

			options.File = args[2];
			if (options.File == null)
				PrintUsageAndExit(-1);

			try
			{
				optionSet.Parse(args);
			}
			catch (Exception e)
			{
				PrintUsageAndExit(e);
			}

			if (options.File != null && Directory.Exists(options.File))
			{
				incremental = true;
			}

			var smugglerApi = new SmugglerApi(connectionStringOptions);

			try
			{
				switch (action)
				{
					case SmugglerAction.Import:
						smugglerApi.ImportData(options, incremental);
						break;
					case SmugglerAction.Export:
						smugglerApi.ExportData(options, incremental);
						break;
				}
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
				Environment.Exit(-1);
			}
		}

		private void PrintUsageAndExit(Exception e)
		{
			Console.WriteLine(e.Message);
			PrintUsageAndExit(-1);
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