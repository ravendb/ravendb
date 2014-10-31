//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics.Eventing.Reader;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using NDesk.Options;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Smuggler
{
	public class Program
	{
        private readonly SmugglerDatabaseApi smugglerApi = new SmugglerDatabaseApi();
        private readonly SmugglerFilesApi smugglerFilesApi = new SmugglerFilesApi();
        
        private readonly OptionSet optionSet;
        private readonly OptionSet selectionDispatching;
	    private bool allowImplicitDatabase = false;

	    private Program()
	    {
            var databaseOptions = smugglerApi.Options;
            var filesOptions = smugglerFilesApi.Options;

	        selectionDispatching = new OptionSet
	        {
			    {"d|d2|database|database2:", value =>
			                    {
			                        if (mode == SmugglerMode.Unknown || mode == SmugglerMode.Database)
			                            mode = SmugglerMode.Database;
			                        else PrintUsageAndExit(new ArgumentException("Database and Filesystem parameters are mixed. You cannot use both in the same request."));
			                    } 
                },
			    {"f|f2|filesystem|filesystem2:", value =>
			                    {
                                    if (mode == SmugglerMode.Unknown || mode == SmugglerMode.Filesystem)
                                        mode = SmugglerMode.Filesystem;
			                        else PrintUsageAndExit(new ArgumentException("Database and Filesystem parameters are mixed. You cannot use both in the same request."));
			                    }
                },
	        };

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
							    databaseOptions.OperateOnTypes = (ItemType) Enum.Parse(typeof (ItemType), value, ignoreCase: true);
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
			            			                       "Usage example: Raven-Entity-Name=Posts, or Raven-Entity-Name=Posts,Persons for multiple document types", (key, val) => databaseOptions.Filters.Add(new FilterSetting
				    {
					    Path = "@metadata." + key,
					    ShouldMatch = true,
						Values = FilterSetting.ParseValues(val)
				    })
			    },
			    {
				    "negative-metadata-filter:{=}", "Filter documents NOT matching a metadata property." + Environment.NewLine +
				                                    "Usage example: Raven-Entity-Name=Posts",
				    (key, val) => databaseOptions.Filters.Add(
						new FilterSetting
						{
							Path = "@metadata." + key,
							ShouldMatch = false,
							Values = FilterSetting.ParseValues(val)
						})
			    },
			    {
				    "filter:{=}", "Filter documents by a document property" + Environment.NewLine +
				                  "Usage example: Property-Name=Value",
				    (key, val) => databaseOptions.Filters.Add(
						new FilterSetting
						{
							Path = key,
							ShouldMatch = true,
							Values = FilterSetting.ParseValues(val)
						})
			    },
			    {
				    "negative-filter:{=}", "Filter documents NOT matching a document property" + Environment.NewLine +
				                           "Usage example: Property-Name=Value",
				    (key, val) => databaseOptions.Filters.Add(
						new FilterSetting
						{
							Path = key,
							ShouldMatch = false,
							Values = FilterSetting.ParseValues(val)
						})
			    },
			    {
				    "transform:", "Transform documents using a given script (import only)", script => databaseOptions.TransformScript = script
			    },
			    {
				    "transform-file:", "Transform documents using a given script file (import only)", script => databaseOptions.TransformScript = File.ReadAllText(script)
			    },
			    {
				    "max-steps-for-transform-script:", "Maximum number of steps that transform script can have (import only)", s => databaseOptions.MaxStepsForTransformScript = int.Parse(s)
			    },

			    {"batch-size:", "The batch size for requests", s => databaseOptions.BatchSize = int.Parse(s)},
				{"chunk-size:", "The number of documents to import before new connection will be opened", s => databaseOptions.ChunkSize = int.Parse(s)},
			    {"d|database:", "The database to operate on. If no specified, the operations will be on the default database.", value => databaseOptions.Source.DefaultDatabase = value},
			    {"d2|database2:", "The database to export to. If no specified, the operations will be on the default database. This parameter is used only in the between operation.", value => databaseOptions.Destination.DefaultDatabase = value},
                {"wait-for-indexing", "Wait until all indexing activity has been completed (import only)", _ => databaseOptions.WaitForIndexing = true},
                {"excludeexpired", "Excludes expired documents created by the expiration bundle", _ => databaseOptions.ShouldExcludeExpired = true},
                {"limit:", "Reads at most VALUE documents/attachments.", s => databaseOptions.Limit = int.Parse(s)},

                // Common
                {"h|?|help", v => PrintUsageAndExit(0)},
                {"timeout:", "The timeout to use for requests", s => 
                    {
                        databaseOptions.Timeout = TimeSpan.FromMilliseconds(int.Parse(s));
                        filesOptions.Timeout = TimeSpan.FromMilliseconds(int.Parse(s));
                    }},
                {"incremental", "States usage of incremental operations", _ => 
                    {
                        databaseOptions.Incremental = true;
                        filesOptions.Incremental = true;
                    }},
			    {"u|user|username:", "The username to use when the database/filesystem requires the client to authenticate.", value => {
                    ((NetworkCredential)databaseOptions.Source.Credentials).UserName = value;
                    ((NetworkCredential)filesOptions.Source.Credentials).UserName = value;
                }},
			    {"u2|user2|username2:", "The username to use when the database/filesystem requires the client to authenticate. This parameter is used only in the between operation.", value =>{
                    ((NetworkCredential)databaseOptions.Destination.Credentials).UserName = value;
                    ((NetworkCredential)filesOptions.Destination.Credentials).UserName = value;
                }},
			    {"p|pass|password:", "The password to use when the database/filesystem requires the client to authenticate.", value => {
                    ((NetworkCredential) databaseOptions.Source.Credentials).Password = value;
                    ((NetworkCredential)filesOptions.Source.Credentials).Password = value;
                }},
			    {"p2|pass2|password2:", "The password to use when the database/filesystem requires the client to authenticate. This parameter is used only in the between operation.", value => {
                    ((NetworkCredential)databaseOptions.Destination.Credentials).Password = value;
                    ((NetworkCredential)filesOptions.Destination.Credentials).Password = value;
                } },
			    {"domain:", "The domain to use when the database/filesystem requires the client to authenticate.", value => {
                    ((NetworkCredential)databaseOptions.Source.Credentials).Domain = value;
                    ((NetworkCredential)filesOptions.Source.Credentials).Domain = value;
                }},
			    {"domain2:", "The domain to use when the database/filesystem requires the client to authenticate. This parameter is used only in the between operation.", value => {
                    ((NetworkCredential)databaseOptions.Destination.Credentials).Domain = value;
                    ((NetworkCredential)filesOptions.Destination.Credentials).Domain = value;
                }},
			    {"key|api-key|apikey:", "The API-key to use, when using OAuth.", value => {
                    databaseOptions.Source.ApiKey = value;
                    filesOptions.Source.ApiKey = value;
                }},
			    {"key2|api-key2|apikey2:", "The API-key to use, when using OAuth. This parameter is used only in the between operation.", value => {
                    databaseOptions.Destination.ApiKey = value;
                    filesOptions.Destination.ApiKey = value;
                }},


                // Filesystem ONLY!
                {"f|filesystem:", "The filesystem to operate on. If no specified, the operations will be on the default filesystem.", value => filesOptions.Source.DefaultFileSystem = value},
			    {"f2|filesystem2:", "The filesystem to export to. If no specified, the operations will be on the default filesystem. This parameter is used only in the between operation.", value => filesOptions.Destination.DefaultFileSystem = value},

		    };
	    }

		static void Main(string[] args)
		{
			var program = new Program();
			program.Parse(args).Wait();
		}

        private SmugglerMode mode = SmugglerMode.Unknown;

        private async Task Parse(string[] args)
		{
            var options = smugglerApi.Options;
            var filesOptions = smugglerFilesApi.Options;

			// Do these arguments the traditional way to maintain compatibility
			if (args.Length < 3)
				PrintUsageAndExit(-1);

            try
            {
                selectionDispatching.Parse(args);
            }
            catch (Exception e)
            {
                PrintUsageAndExit(e);
            }

            var url = args[1];
            if (url == null || args[2] == null )
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

            if (action != SmugglerAction.Between && Directory.Exists(options.BackupPath))
			{
                smugglerApi.Options.Incremental = true;
			}

			try
			{
                optionSet.Parse(args);
			}
            catch (Exception e)
			{
                PrintUsageAndExit(e);
			}

            switch (this.mode)
            {
                case SmugglerMode.Database:
                    {
                        options.Source.Url = url;
                        options.BackupPath = args[2];

                        ValidateDatabaseParameters(smugglerApi, action);
                        var databaseDispatcher = new SmugglerDatabaseOperationDispatcher(smugglerApi);
                        await databaseDispatcher.Execute(action);
                    }
                    break;
                case SmugglerMode.Filesystem:
                    {
                        filesOptions.Source.Url = url;
                        filesOptions.BackupPath = args[2];

                        var filesDispatcher = new SmugglerFilesOperationDispatcher(smugglerFilesApi);
                        await filesDispatcher.Execute(action);
                    }
                    break;
            }
        }

        private void ValidateDatabaseParameters(SmugglerDatabaseApi api, SmugglerAction action)
        {
            if (allowImplicitDatabase == false)
            {
                if (string.IsNullOrEmpty(api.Options.Source.DefaultDatabase))
                {
                    throw new OptionException("--database parameter must be specified or pass --allow-implicit-database", "database");
                }

                if (action == SmugglerAction.Between && string.IsNullOrEmpty(api.Options.Destination.DefaultDatabase))
                {
                    throw new OptionException("--database2 parameter must be specified or pass --allow-implicit-database", "database2");
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
		Raven.Smuggler between http://localhost:8080/  http://localhost:8081/ --database=sourceDB --database2=targetDB
    - Import a file system dump.ravenfs file to the MyFiles filesystem of the specified RavenDB instance:
		Raven.Smuggler in http://localhost:8080/ dump.ravenfs --filesystem=MyFiles
	- Export from MyFiles file system of the specified RavenDB instance to the dump.ravenfs file:
		Raven.Smuggler out http://localhost:8080/ dump.ravenfs --filesystem=MyFiles
	- Export from MyFiles1 to MyFiles2 on a different RavenDB instance:
		Raven.Smuggler between http://localhost:8080/ http://localhost:8081/ --filesystem=sourceDB --filesystem2=targetDB
    

Command line options:", SystemTime.UtcNow.Year);

			optionSet.WriteOptionDescriptions(Console.Out);
			Console.WriteLine();

			Environment.Exit(exitCode);
		}
	}
}
