//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NDesk.Options;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;

namespace Raven.Smuggler
{
    using System.Linq;
	using System.Net.Sockets;

	public class Program
	{
		private readonly RavenConnectionStringOptions connectionStringOptions = new RavenConnectionStringOptions {Credentials = new NetworkCredential()};
		private readonly RavenConnectionStringOptions connectionStringOptions2 = new RavenConnectionStringOptions {Credentials = new NetworkCredential()};
		private readonly SmugglerOptions options = new SmugglerOptions();
		private readonly OptionSet optionSet;
		bool waitForIndexing;

	    private Program()
	    {
		    optionSet = new OptionSet
		    {
			    {
				    "operate-on-types:", "Specify the types to operate on. Specify the types to operate on. You can specify more than one type by combining items with a comma." + Environment.NewLine +
				                         "Default is all items." + Environment.NewLine +
				                         "Usage example: Indexes,Documents,Attachments",
				    value =>
				    {
					    try
					    {
						    if (string.IsNullOrWhiteSpace(value) == false)
						    {
							    options.OperateOnTypes = (ItemType) Enum.Parse(typeof (ItemType), value, ignoreCase: true);
						    }
					    }
					    catch (Exception e)
					    {
						    PrintUsageAndExit(e);
					    }
				    }
			    },
			    {
				    "metadata-filter:{=}", "Filter documents by a metadata property." + Environment.NewLine +
			            			                       "Usage example: Raven-Entity-Name=Posts, or Raven-Entity-Name=Posts,Persons for multiple document types", (key, val) => options.Filters.Add(new FilterSetting
				    {
					    Path = "@metadata." + key,
					    ShouldMatch = true,
															   Values = FilterSetting.ParseValues(val)
				    })
			    },
			    {
				    "negative-metadata-filter:{=}", "Filter documents NOT matching a metadata property." + Environment.NewLine +
				                                    "Usage example: Raven-Entity-Name=Posts",
				    (key, val) => options.Filters.Add(new FilterSetting
				    {
					    Path = "@metadata." + key,
					    ShouldMatch = false,
															   Values = FilterSetting.ParseValues(val)
				    })
			    },
			    {
				    "filter:{=}", "Filter documents by a document property" + Environment.NewLine +
				                  "Usage example: Property-Name=Value",
				    (key, val) => options.Filters.Add(new FilterSetting
				    {
					    Path = key,
					    ShouldMatch = true,
													  Values = FilterSetting.ParseValues(val)
				    })
			    },
			    {
				    "negative-filter:{=}", "Filter documents NOT matching a document property" + Environment.NewLine +
				                           "Usage example: Property-Name=Value",
				    (key, val) => options.Filters.Add(new FilterSetting
				    {
					    Path = key,
					    ShouldMatch = false,
													  Values = FilterSetting.ParseValues(val)
				    })
			    },
			    {
				    "transform:", "Transform documents using a given script (import only)", script => options.TransformScript = script
			    },
			    {
				    "transform-file:", "Transform documents using a given script file (import only)", script => options.TransformScript = File.ReadAllText(script)
			    },
			    {
				    "max-steps-for-transform-script:", "Maximum number of steps that transform script can have (import only)", s => options.MaxStepsForTransformScript = int.Parse(s)
			    },
			    {"timeout:", "The timeout to use for requests", s => options.Timeout = TimeSpan.FromMilliseconds(int.Parse(s))},
			    {"batch-size:", "The batch size for requests", s => options.BatchSize = int.Parse(s)},
			    {"d|database:", "The database to operate on. If no specified, the operations will be on the default database.", value => connectionStringOptions.DefaultDatabase = value},
			    {"d2|database2:", "The database to export to. If no specified, the operations will be on the default database. This parameter is used only in the between operation.", value => connectionStringOptions2.DefaultDatabase = value},
			    {"u|user|username:", "The username to use when the database requires the client to authenticate.", value => ((NetworkCredential) connectionStringOptions.Credentials).UserName = value},
			    {"u2|user2|username2:", "The username to use when the database requires the client to authenticate. This parameter is used only in the between operation.", value => ((NetworkCredential) connectionStringOptions2.Credentials).UserName = value},
			    {"p|pass|password:", "The password to use when the database requires the client to authenticate.", value => ((NetworkCredential) connectionStringOptions.Credentials).Password = value},
			    {"p2|pass2|password2:", "The password to use when the database requires the client to authenticate. This parameter is used only in the between operation.", value => ((NetworkCredential) connectionStringOptions2.Credentials).Password = value},
			    {"domain:", "The domain to use when the database requires the client to authenticate.", value => ((NetworkCredential) connectionStringOptions.Credentials).Domain = value},
			    {"domain2:", "The domain to use when the database requires the client to authenticate. This parameter is used only in the between operation.", value => ((NetworkCredential) connectionStringOptions2.Credentials).Domain = value},
			    {"key|api-key|apikey:", "The API-key to use, when using OAuth.", value => connectionStringOptions.ApiKey = value},
			    {"key2|api-key2|apikey2:", "The API-key to use, when using OAuth. This parameter is used only in the between operation.", value => connectionStringOptions2.ApiKey = value},
			    {"incremental", "States usage of incremental operations", _ => options.Incremental = true},
			    {"wait-for-indexing", "Wait until all indexing activity has been completed (import only)", _ => waitForIndexing = true},
                {"excludeexpired", "Excludes expired documents created by the expiration bundle", _ => options.ShouldExcludeExpired = true},
                {"limit:", "Reads at most VALUE documents/attachments.", s => options.Limit = int.Parse(s)},
			    {"h|?|help", v => PrintUsageAndExit(0)},
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

            var url = args[1];
            if (url == null)
            {
                PrintUsageAndExit(-1);
                return;
            }
            connectionStringOptions.Url = url;

            var backupPath = args[2];
            if (backupPath == null)
                PrintUsageAndExit(-1);

			SmugglerAction action;
		    if (string.Equals(args[0], "in", StringComparison.OrdinalIgnoreCase))
		    {
		        action = SmugglerAction.Import;
		    }
			else if (string.Equals(args[0], "out", StringComparison.OrdinalIgnoreCase))
			{
			    action = SmugglerAction.Export;
			}
            else if (string.Equals(args[0], "between", StringComparison.OrdinalIgnoreCase))
            {
                action = SmugglerAction.Between;
            }
            else
            {
                PrintUsageAndExit(-1);
                return;
            }

			if (action != SmugglerAction.Between && Directory.Exists(backupPath))
			{
				options.Incremental = true;
			}

			try
			{
				optionSet.Parse(args);
			}
			catch (Exception e)
			{
				PrintUsageAndExit(e);
			}

            var smugglerApi = new SmugglerApi();

			try
			{
				switch (action)
				{
					case SmugglerAction.Import:
						smugglerApi.ImportData(new SmugglerImportOptions {FromFile = backupPath}, options).Wait();
						if (waitForIndexing)
							smugglerApi.WaitForIndexing(options).Wait();
						break;
					case SmugglerAction.Export:
                        smugglerApi.ExportData(new SmugglerExportOptions {ToFile = backupPath}, options).Wait();
						break;
                    case SmugglerAction.Between:
						connectionStringOptions2.Url = backupPath;
						SmugglerOperation.Between(new SmugglerBetweenOptions {From = connectionStringOptions, To = connectionStringOptions2}, options).Wait();
						break;
				}
			}
			catch (AggregateException ex)
			{
			    var exception = ex.ExtractSingleInnerException();
			    var e = exception as WebException;
			    if (e != null)
			    {

					if (e.Status == WebExceptionStatus.ConnectFailure)
					{
						Console.WriteLine("Error: {0} {1}", e.Message, connectionStringOptions.Url + (action == SmugglerAction.Between ? " => " + connectionStringOptions2.Url : ""));
						var socketException = e.InnerException as SocketException;
						if (socketException != null)
						{
							Console.WriteLine("Details: {0}", socketException.Message);
							Console.WriteLine("Socket Error Code: {0}", socketException.SocketErrorCode);
						}

			            Environment.Exit((int) e.Status);
			        }

			        var httpWebResponse = e.Response as HttpWebResponse;
			        if (httpWebResponse == null)
			            throw;
			        Console.WriteLine("Error: " + e.Message);
			        Console.WriteLine("Http Status Code: " + httpWebResponse.StatusCode + " " + httpWebResponse.StatusDescription);

			        using (var reader = new StreamReader(httpWebResponse.GetResponseStream()))
			        {
			            string line;
			            while ((line = reader.ReadLine()) != null)
			            {
			                Console.WriteLine(line);
			            }
			        }

			        Environment.Exit((int) httpWebResponse.StatusCode);
			    }
			    else
			    {
			        Console.WriteLine(ex);
			        Environment.Exit(-1);
			    }
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
	- Import the dump.raven file to the MyDatabase database of the specified RavenDB instance:
		Raven.Smuggler in http://localhost:8080/ dump.raven --database=MyDatabase
	- Export from MyDatabase database of the specified RavenDB instance to the dump.raven file:
		Raven.Smuggler out http://localhost:8080/ dump.raven --database=MyDatabase
	- Export from Database1 to Database2 on a different RavenDB instance:
		Raven.Smuggler between http://localhost:8080/databases/Database1 http://localhost:8081/databases/Database2

Command line options:", SystemTime.UtcNow.Year);

			optionSet.WriteOptionDescriptions(Console.Out);
			Console.WriteLine();

			Environment.Exit(exitCode);
		}
	}
}
