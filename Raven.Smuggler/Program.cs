//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Net;
using NDesk.Options;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;

namespace Raven.Smuggler
{
    using System.Net.Sockets;

    public class Program
    {
        private readonly RavenConnectionStringOptions connectionStringOptions;
        private readonly SmugglerOptions options;
        private readonly OptionSet optionSet;
        bool incremental, waitForIndexing;

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
			            			                                                                     			options.OperateOnTypes = options.ItemTypeParser(value);
			            			                                                                     		}
			            			                                                                     		catch (Exception e)
			            			                                                                     		{
			            			                                                                     			PrintUsageAndExit(e);
			            			                                                                     		}
			            			                                                                     	}
			            			},
			            		{
			            			"metadata-filter:{=}", "Filter documents by a metadata property." + Environment.NewLine +
			            			                       "Usage example: Raven-Entity-Name=Posts", (key, val) => options.Filters.Add(new FilterSetting
			            			                       {
				            			                       Path = "@metadata." + key,
															   ShouldMatch = true,
															   Value = val
			            			                       })
			            			},
								{
			            			"negative-metadata-filter:{=}", "Filter documents NOT matching a metadata property." + Environment.NewLine +
			            			                       "Usage example: Raven-Entity-Name=Posts", (key, val) => options.Filters.Add(new FilterSetting
			            			                       {
				            			                       Path = "@metadata." + key,
															   ShouldMatch = false,
															   Value = val
			            			                       })
			            			},
			            		{
			            			"filter:{=}", "Filter documents by a document property" + Environment.NewLine +
			            			              "Usage example: Property-Name=Value", (key, val) => options.Filters.Add(new FilterSetting
			            			              {
													  Path = key,
													  ShouldMatch = true,
				            			              Value = val
			            			              })
			            			},
								{
			            			"negative-filter:{=}", "Filter documents NOT matching a document property" + Environment.NewLine +
			            			              "Usage example: Property-Name=Value", (key, val) => options.Filters.Add(new FilterSetting
			            			              {
													  Path = key,
													  ShouldMatch = true,
				            			              Value = val
			            			              })
			            			},
								{"timeout:", "The timeout to use for requests", s => options.Timeout = int.Parse(s) },
								{"batch-size:", "The batch size for requests", s => options.BatchSize = int.Parse(s) },
			            		{"d|database:", "The database to operate on. If no specified, the operations will be on the default database.", value => connectionStringOptions.DefaultDatabase = value},
			            		{"u|user|username:", "The username to use when the database requires the client to authenticate.", value => Credentials.UserName = value},
			            		{"p|pass|password:", "The password to use when the database requires the client to authenticate.", value => Credentials.Password = value},
			            		{"domain:", "The domain to use when the database requires the client to authenticate.", value => Credentials.Domain = value},
			            		{"key|api-key|apikey:", "The API-key to use, when using OAuth.", value => connectionStringOptions.ApiKey = value},
								{"incremental", "States usage of incremental operations", _ => incremental = true },
								{"wait-for-indexing", "Wait until all indexing activity has been completed (import only)", _=> waitForIndexing=true},
                                {"excludeexpired", "Excludes expired documents created by the expiration bundle", _ => options.ShouldExcludeExpired = true },
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
            if (args.Length < 2)
                PrintUsageAndExit(-1);

            var action = SmugglerAction.Export;
            switch (args[0].ToLowerInvariant())
            {
                case "in":
                    action = SmugglerAction.Import;
                    break;

                case "out":
                    action = SmugglerAction.Export;
                    break;

                case "dryrun":
                    action = SmugglerAction.Dryrun;
                    break;

                case "repair":
                    action = SmugglerAction.Repair;
                    break;

                default:
                    PrintUsageAndExit(-1);
                    break;
            }

            if (action == SmugglerAction.Import || action == SmugglerAction.Export)
            {
                var url = args[1];
                if (url == null)
                {
                    PrintUsageAndExit(-1);
                    return;
                }
                connectionStringOptions.Url = url;

                options.BackupPath = args[2];
            }
            else
            {
                options.BackupPath = args[1];
            }

            if (options.BackupPath == null)
                PrintUsageAndExit(-1);

            try
            {
                optionSet.Parse(args);
            }
            catch (Exception e)
            {
                PrintUsageAndExit(e);
            }

            if (options.BackupPath != null && Directory.Exists(options.BackupPath))
            {
                incremental = true;
            }

            var smugglerApi = SmugglerApiFactory.Create(action, options, connectionStringOptions);

            try
            {
                switch (action)
                {
                    case SmugglerAction.Import:
                    case SmugglerAction.Dryrun:
                    case SmugglerAction.Repair:
                        smugglerApi.ImportData(options, incremental);
                        if (waitForIndexing)
                            smugglerApi.WaitForIndexing(options);
                        break;
                    case SmugglerAction.Export:
                        smugglerApi.ExportData(options, incremental);
                        break;
                }
            }
            catch (WebException e)
            {
                if (e.Status == WebExceptionStatus.ConnectFailure)
                {
                    Console.WriteLine("Error: {0} {1}", e.Message, connectionStringOptions.Url);
                    var socketException = e.InnerException as SocketException;
                    if (socketException != null)
                    {
                        Console.WriteLine("Details: {0}", socketException.Message);
                        Console.WriteLine("Socket Error Code: {0}", socketException.SocketErrorCode);
                    }

                    Environment.Exit((int)e.Status);
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

                Environment.Exit((int)httpWebResponse.StatusCode);
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
	- Dump the dump.raven file to stdout
		Raven.Smuggler dryrun dump.raven
	- Repair the dump.raven to dump.raven.repair
		Raven.Smuggler repair dump.raven    

Command line options:", SystemTime.UtcNow.Year);

            optionSet.WriteOptionDescriptions(Console.Out);
            Console.WriteLine();

            Environment.Exit(exitCode);
        }
    }
}
