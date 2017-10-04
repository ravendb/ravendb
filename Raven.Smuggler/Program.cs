//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Net.Http;
using System.Runtime.Remoting.Messaging;
using NDesk.Options;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Extensions;
using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;
using Raven.Database.Smuggler;
using Raven.Smuggler.Helpers;

namespace Raven.Smuggler
{
    public class Program
    {
        private readonly SmugglerDatabaseApi smugglerApi = new SmugglerDatabaseApi();
        private readonly SmugglerFilesApi smugglerFilesApi = new SmugglerFilesApi();
        private readonly SmugglerCounterApi smugglerCounterApi = new SmugglerCounterApi();

        private OptionSet databaseOptionSet;
        private OptionSet filesystemOptionSet;
        private OptionSet counterOptionSet;
        private readonly OptionSet selectionDispatching;
        private bool allowOperationOnSystemDatabase;

        private Program()
        {
            var databaseOptions = smugglerApi.Options;
            var filesOptions = smugglerFilesApi.Options;
            var counterOptions = smugglerCounterApi.Options;

            selectionDispatching = new OptionSet();
            selectionDispatching.OnWarning += s => ConsoleHelper.WriteLineWithColor(ConsoleColor.Yellow, s);
            selectionDispatching.Add("nc|no-compression-on-import:", OptionCategory.None, "A flag that if set disables compression usage during import of documents.", value =>
            {
                bool disableCompression;
                if (String.IsNullOrWhiteSpace(value) == false &&
                    Boolean.TryParse(value, out disableCompression))
                    databaseOptions.DisableCompressionOnImport = disableCompression;
                else
                    PrintUsageAndExit(new ArgumentException("Invalid value for no-compression-on-import flag. Only 'true' and 'false' values should be used."));
            });

            selectionDispatching.Add("d|d2|database|database2:", OptionCategory.None, string.Empty, value =>
            {
                if (mode == SmugglerMode.Unknown || mode == SmugglerMode.Database)
                    mode = SmugglerMode.Database;
                else PrintUsageAndExit(new ArgumentException("Parameters for Database and for other storage types are mixed. You cannot use multiple in the same request."));
            });

            selectionDispatching.Add("f|f2|filesystem|filesystem2:", OptionCategory.None, string.Empty, value =>
            {
                if (mode == SmugglerMode.Unknown || mode == SmugglerMode.Filesystem)
                    mode = SmugglerMode.Filesystem;
                else PrintUsageAndExit(new ArgumentException("Parameters for Filesystem and for other storage types are mixed. You cannot use multiple in the same request."));
            });

            selectionDispatching.Add("c|c2|counter|counter2:", OptionCategory.None, string.Empty, value =>
            {
                if (mode == SmugglerMode.Unknown || mode == SmugglerMode.Counter)
                    mode = SmugglerMode.Counter;
                else PrintUsageAndExit(new ArgumentException("Parameters for Counter and for other storage types are mixed. You cannot use multiple in the same request."));
            });

            DefineDatabaseOptionsSet(databaseOptions);
            DefineFilesystemOptionSet(filesOptions);
            DefineCounterOptionSet(counterOptions);
        }

        private string counterStorageName;
        private string counterStorageName2;

        private void DefineCounterOptionSet(SmugglerCounterOptions counterOptions)
        {
            counterOptionSet = new OptionSet();
            counterOptionSet.OnWarning += s => ConsoleHelper.WriteLineWithColor(ConsoleColor.Yellow, s);
            counterOptionSet.Add("timeout:", OptionCategory.SmugglerCounter, "The timeout to use for requests", s => counterOptions.Timeout = TimeSpan.FromMilliseconds(int.Parse(s)));
            counterOptionSet.Add("incremental", OptionCategory.SmugglerCounter, "States usage of incremental operations", _ => counterOptions.Incremental = true);
            counterOptionSet.Add("u|user|username:", OptionCategory.SmugglerCounter, "The username to use when the counter storage requires the client to authenticate.", value => GetCredentials(counterOptions.Source).UserName = value);
            counterOptionSet.Add("u2|user2|username2:", OptionCategory.SmugglerCounter, "The username to use when the counter storage requires the client to authenticate. This parameter is used only in the between operation.", value => GetCredentials(counterOptions.Destination).UserName = value);
            counterOptionSet.Add("p|pass|password:", OptionCategory.SmugglerCounter, "The password to use when the counter storage requires the client to authenticate.", value => GetCredentials(counterOptions.Source).Password = value);
            counterOptionSet.Add("p2|pass2|password2:", OptionCategory.SmugglerCounter, "The password to use when the counter storage requires the client to authenticate. This parameter is used only in the between operation.", value => GetCredentials(counterOptions.Destination).Password = value);
            counterOptionSet.Add("domain:", OptionCategory.SmugglerCounter, "The domain to use when the counter storage requires the client to authenticate.", value => GetCredentials(counterOptions.Source).Domain = value);
            counterOptionSet.Add("domain2:", OptionCategory.SmugglerCounter, "The domain to use when the counter storage requires the client to authenticate. This parameter is used only in the between operation.", value => GetCredentials(counterOptions.Destination).Domain = value);
            counterOptionSet.Add("key|api-key|apikey:", OptionCategory.SmugglerCounter, "The API-key to use, when using OAuth.", value => counterOptions.Source.ApiKey = value);
            counterOptionSet.Add("key2|api-key2|apikey2:", OptionCategory.SmugglerCounter, "The API-key to use, when using OAuth. This parameter is used only in the between operation.", value => counterOptions.Destination.ApiKey = value);
            counterOptionSet.Add("c|counter:", OptionCategory.SmugglerCounter, "The counter storage to operate on. If no specified, the operations will be on the default counter storage.", value => counterStorageName = value);
            counterOptionSet.Add("c2|counter2:", OptionCategory.SmugglerCounter, "The counter storage to export to. If no specified, the operations will be on the default counter storage. This parameter is used only in the between operation.", value => counterStorageName2 = value);
        }

        private void DefineFilesystemOptionSet(SmugglerFilesOptions filesOptions)
        {
            filesystemOptionSet = new OptionSet();
            filesystemOptionSet.OnWarning += s => ConsoleHelper.WriteLineWithColor(ConsoleColor.Yellow, s);
            filesystemOptionSet.Add("timeout:", OptionCategory.SmugglerFileSystem, "The timeout to use for requests", s => filesOptions.Timeout = TimeSpan.FromMilliseconds(int.Parse(s)));
            filesystemOptionSet.Add("incremental", OptionCategory.SmugglerFileSystem, "States usage of incremental operations", _ => filesOptions.Incremental = true);
            filesystemOptionSet.Add("disable-versioning-during-import", OptionCategory.SmugglerFileSystem, "Disables versioning for the duration of the import", _ => filesOptions.ShouldDisableVersioningBundle = true);
            filesystemOptionSet.Add("u|user|username:", OptionCategory.SmugglerFileSystem, "The username to use when the filesystem requires the client to authenticate.", value => GetCredentials(filesOptions.Source).UserName = value);
            filesystemOptionSet.Add("u2|user2|username2:", OptionCategory.SmugglerFileSystem, "The username to use when the filesystem requires the client to authenticate. This parameter is used only in the between operation.", value => GetCredentials(filesOptions.Destination).UserName = value);
            filesystemOptionSet.Add("p|pass|password:", OptionCategory.SmugglerFileSystem, "The password to use when the filesystem requires the client to authenticate.", value => GetCredentials(filesOptions.Source).Password = value);
            filesystemOptionSet.Add("p2|pass2|password2:", OptionCategory.SmugglerFileSystem, "The password to use when the filesystem requires the client to authenticate. This parameter is used only in the between operation.", value => GetCredentials(filesOptions.Destination).Password = value);
            filesystemOptionSet.Add("domain:", OptionCategory.SmugglerFileSystem, "The domain to use when the filesystem requires the client to authenticate.", value => GetCredentials(filesOptions.Source).Domain = value);
            filesystemOptionSet.Add("domain2:", OptionCategory.SmugglerFileSystem, "The domain to use when the filesystem requires the client to authenticate. This parameter is used only in the between operation.", value => GetCredentials(filesOptions.Destination).Domain = value);
            filesystemOptionSet.Add("key|api-key|apikey:", OptionCategory.SmugglerFileSystem, "The API-key to use, when using OAuth.", value => filesOptions.Source.ApiKey = value);
            filesystemOptionSet.Add("key2|api-key2|apikey2:", OptionCategory.SmugglerFileSystem, "The API-key to use, when using OAuth. This parameter is used only in the between operation.", value => filesOptions.Destination.ApiKey = value);
            filesystemOptionSet.Add("f|filesystem:", OptionCategory.SmugglerFileSystem, "The filesystem to operate on. If no specified, the operations will be on the default filesystem.", value => filesOptions.Source.DefaultFileSystem = value);
            filesystemOptionSet.Add("f2|filesystem2:", OptionCategory.SmugglerFileSystem, "The filesystem to export to. If no specified, the operations will be on the default filesystem. This parameter is used only in the between operation.", value => filesOptions.Destination.DefaultFileSystem = value);
            filesystemOptionSet.Add("batch-size:", OptionCategory.SmugglerFileSystem, "The batch size for requests", s => filesOptions.BatchSize = int.Parse(s));
        }

        private void DefineDatabaseOptionsSet(SmugglerDatabaseOptions databaseOptions)
        {
            databaseOptionSet = new OptionSet();
            databaseOptionSet.OnWarning += s => ConsoleHelper.WriteLineWithColor(ConsoleColor.Yellow, s);
            databaseOptionSet.Add("operate-on-types:", OptionCategory.SmugglerDatabase, "Specify the types to operate on. Specify the types to operate on. You can specify more than one type by combining items with a comma." + Environment.NewLine +
                                                       "Default is all items." + Environment.NewLine +
                                                       "Usage example: Indexes,Documents,Attachments,RemoveAnalyzers", value =>
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
                                                       });

            databaseOptionSet.Add("metadata-filter:{=}", OptionCategory.SmugglerDatabase, "Filter documents by a metadata property." + Environment.NewLine +
                                                         "Usage example: Raven-Entity-Name=Posts, or Raven-Entity-Name=Posts,Persons for multiple document types", (key, val) => databaseOptions.Filters.Add(new FilterSetting
                                                                                                                                                                                                             {
                                                                                                                                                                                                                 Path = "@metadata." + key,
                                                                                                                                                                                                                 ShouldMatch = true,
                                                                                                                                                                                                                 Values = FilterSetting.ParseValues(val)
                                                                                                                                                                                                             }));

            databaseOptionSet.Add("negative-metadata-filter:{=}", OptionCategory.SmugglerDatabase, "Filter documents NOT matching a metadata property." + Environment.NewLine +
                                                                  "Usage example: Raven-Entity-Name=Posts", (key, val) => databaseOptions.Filters.Add(
                                                                      new FilterSetting
                                                                      {
                                                                          Path = "@metadata." + key,
                                                                          ShouldMatch = false,
                                                                          Values = FilterSetting.ParseValues(val)
                                                                      }));

            databaseOptionSet.Add("filter:{=}", OptionCategory.SmugglerDatabase, "Filter documents by a document property" + Environment.NewLine +
                                                "Usage example: Property-Name=Value", (key, val) => databaseOptions.Filters.Add(
                                                    new FilterSetting
                                                    {
                                                        Path = key,
                                                        ShouldMatch = true,
                                                        Values = FilterSetting.ParseValues(val)
                                                    }));

            databaseOptionSet.Add("negative-filter:{=}", OptionCategory.SmugglerDatabase, "Filter documents NOT matching a document property" + Environment.NewLine +
                                                         "Usage example: Property-Name=Value", (key, val) => databaseOptions.Filters.Add(
                                                             new FilterSetting
                                                             {
                                                                 Path = key,
                                                                 ShouldMatch = false,
                                                                 Values = FilterSetting.ParseValues(val)
                                                             }));

            databaseOptionSet.Add("ignore-errors-and-continue", OptionCategory.SmugglerDatabase, "If this option is enabled, smuggler will not halt its operation on errors. Errors still will be displayed to the user.", value =>
            {
                databaseOptions.IgnoreErrorsAndContinue = true;
            });
            databaseOptionSet.Add("transform:", OptionCategory.SmugglerDatabase, "Transform documents using a given script (import only)", script => databaseOptions.TransformScript = script);
            databaseOptionSet.Add("transform-file:", OptionCategory.SmugglerDatabase, "Transform documents using a given script file (import only)", script => databaseOptions.TransformScript = File.ReadAllText(script));
            databaseOptionSet.Add("max-steps-for-transform-script:", OptionCategory.SmugglerDatabase, "Maximum number of steps that transform script can have (import only)", s => databaseOptions.MaxStepsForTransformScript = int.Parse(s));
            databaseOptionSet.Add("batch-size:", OptionCategory.SmugglerDatabase, "The batch size for requests", s => databaseOptions.BatchSize = int.Parse(s));
            databaseOptionSet.Add("chunk-size:", OptionCategory.SmugglerDatabase, "The number of documents to import before new connection will be opened", s => databaseOptions.ChunkSize = int.Parse(s));
            databaseOptionSet.Add("d|database:", OptionCategory.SmugglerDatabase, "The database to operate on. If no specified, the operations will be on the default database.", value => databaseOptions.Source.DefaultDatabase = value);
            databaseOptionSet.Add("d2|database2:", OptionCategory.SmugglerDatabase, "The database to export to. If no specified, the operations will be on the default database. This parameter is used only in the between operation.", value => databaseOptions.Destination.DefaultDatabase = value);
            databaseOptionSet.Add("wait-for-indexing", OptionCategory.SmugglerDatabase, "Wait until all indexing activity has been completed (import only)", _ => databaseOptions.WaitForIndexing = true);
            databaseOptionSet.Add("excludeexpired", OptionCategory.SmugglerDatabase, "Excludes expired documents created by the expiration bundle", _ => databaseOptions.ShouldExcludeExpired = true);
            databaseOptionSet.Add("disable-versioning-during-import", OptionCategory.SmugglerDatabase, "Disables versioning for the duration of the import", _ => databaseOptions.ShouldDisableVersioningBundle = true);
            databaseOptionSet.Add("limit:", OptionCategory.SmugglerDatabase, "Reads at most VALUE documents/attachments.", s => databaseOptions.Limit = int.Parse(s));
            databaseOptionSet.Add("max-split-export-file-size:", OptionCategory.SmugglerDatabase, "Split exported file by size (after compression) in MB", s => databaseOptions.MaxSplitExportFileSize = int.Parse(s));
            databaseOptionSet.Add("timeout:", OptionCategory.SmugglerDatabase, "The timeout to use for requests", s => databaseOptions.Timeout = TimeSpan.FromMilliseconds(int.Parse(s)));
            databaseOptionSet.Add("incremental", OptionCategory.SmugglerDatabase, "States usage of incremental operations", _ => databaseOptions.Incremental = true);
            databaseOptionSet.Add("u|user|username:", OptionCategory.SmugglerDatabase, "The username to use when the database requires the client to authenticate.", value => GetCredentials(databaseOptions.Source).UserName = value);
            databaseOptionSet.Add("u2|user2|username2:", OptionCategory.SmugglerDatabase, "The username to use when the database requires the client to authenticate. This parameter is used only in the between operation.", value => GetCredentials(databaseOptions.Destination).UserName = value);
            databaseOptionSet.Add("p|pass|password:", OptionCategory.SmugglerDatabase, "The password to use when the database requires the client to authenticate.", value => GetCredentials(databaseOptions.Source).Password = value);
            databaseOptionSet.Add("p2|pass2|password2:", OptionCategory.SmugglerDatabase, "The password to use when the database requires the client to authenticate. This parameter is used only in the between operation.", value => GetCredentials(databaseOptions.Destination).Password = value);
            databaseOptionSet.Add("domain:", OptionCategory.SmugglerDatabase, "The domain to use when the database requires the client to authenticate.", value => GetCredentials(databaseOptions.Source).Domain = value);
            databaseOptionSet.Add("domain2:", OptionCategory.SmugglerDatabase, "The domain to use when the database requires the client to authenticate. This parameter is used only in the between operation.", value => GetCredentials(databaseOptions.Destination).Domain = value);
            databaseOptionSet.Add("key|api-key|apikey:", OptionCategory.SmugglerDatabase, "The API-key to use, when using OAuth.", value => databaseOptions.Source.ApiKey = value);
            databaseOptionSet.Add("key2|api-key2|apikey2:", OptionCategory.SmugglerDatabase, "The API-key to use, when using OAuth. This parameter is used only in the between operation.", value => databaseOptions.Destination.ApiKey = value);
            databaseOptionSet.Add("strip-replication-information", OptionCategory.SmugglerDatabase, "Remove all replication information from metadata (import only)", _ => databaseOptions.StripReplicationInformation = true);
            databaseOptionSet.Add("continuation-token:", OptionCategory.SmugglerDatabase, "Activates the usage of a continuation token in case of unreliable connections or huge imports", s => databaseOptions.ContinuationToken = s);
            databaseOptionSet.Add("skip-conflicted", OptionCategory.SmugglerDatabase, "The database will issue and error when conflicted documents are put. The default is to alert the user, this allows to skip them to continue.", _ => databaseOptions.SkipConflicted = true);
            databaseOptionSet.Add("system-database", OptionCategory.SmugglerDatabase, "Set to true in order to work on a system database", _ => allowOperationOnSystemDatabase = true);
            databaseOptionSet.Add("start-document-etag:", OptionCategory.SmugglerDatabase, "Start exporting from the specified documents etag", value => databaseOptions.StartDocsEtag = Etag.Parse(value));
            databaseOptionSet.Add("start-document-delete-etag:", OptionCategory.SmugglerDatabase, "Start exporting from the specified document deletion etag", value => databaseOptions.StartDocsDeletionEtag = Etag.Parse(value));
            databaseOptionSet.Add("start-attachment-etag:", OptionCategory.SmugglerDatabase, "Start exporting from the specified attachment deletion etag", value => databaseOptions.StartAttachmentsEtag = Etag.Parse(value));
            databaseOptionSet.Add("start-attachment-delete-etag:", OptionCategory.SmugglerDatabase, "Start exporting from the specified attachment deletion etag", value => databaseOptions.StartAttachmentsDeletionEtag = Etag.Parse(value));
        }

        private NetworkCredential GetCredentials(FilesConnectionStringOptions connectionStringOptions)
        {
            var cred = connectionStringOptions.Credentials as NetworkCredential;
            if (cred != null)
                return cred;
            cred = new NetworkCredential();
            connectionStringOptions.Credentials = cred;
            return cred;
        }

        private NetworkCredential GetCredentials(CounterConnectionStringOptions connectionStringOptions)
        {
            var cred = connectionStringOptions.Credentials as NetworkCredential;
            if (cred != null)
                return cred;
            cred = new NetworkCredential();
            connectionStringOptions.Credentials = cred;
            return cred;
        }

        private static NetworkCredential GetCredentials(RavenConnectionStringOptions connectionStringOptions)
        {
            var cred = connectionStringOptions.Credentials as NetworkCredential;
            if (cred != null)
                return cred;
            cred = new NetworkCredential();
            connectionStringOptions.Credentials = cred;
            return cred;
        }

        private static void Main(string[] args)
        {
            var program = new Program();
            AsyncHelpers.RunSync(() => program.Parse(args));
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
            if (url == null || args[2] == null)
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

            try
            {
                switch (mode)
                {
                    case SmugglerMode.Database:
                    {
                        CallContext.LogicalSetData(Constants.Smuggler.CallContext, true);

                        try
                        {
                            databaseOptionSet.Parse(args);
                        }
                        catch (Exception e)
                        {
                            PrintUsageAndExit(e);
                        }

                        options.Source.Url = url;
                        options.BackupPath = args[2];

                        if (action != SmugglerAction.Between && Directory.Exists(options.BackupPath))
                            smugglerApi.Options.Incremental = true;

                        if (NetworkUtil.IsLocalhost(smugglerApi.Options.Destination.Url) ||
                            NetworkUtil.IsLocalhost(smugglerApi.Options.BackupPath))
                            smugglerApi.Options.DisableCompressionOnImport = true;

                        ValidateDatabaseParameters(smugglerApi, action);
                        var databaseDispatcher = new SmugglerDatabaseOperationDispatcher(smugglerApi);
                        await databaseDispatcher.Execute(action).ConfigureAwait(false);
                    }
                        break;
                    case SmugglerMode.Filesystem:
                    {
                        try
                        {
                            filesystemOptionSet.Parse(args);
                        }
                        catch (Exception e)
                        {
                            PrintUsageAndExit(e);
                        }

                        filesOptions.Source.Url = url;
                        filesOptions.BackupPath = args[2];

                        if (action != SmugglerAction.Between && Directory.Exists(options.BackupPath))
                            smugglerFilesApi.Options.Incremental = true;

                        var filesDispatcher = new SmugglerFilesOperationDispatcher(smugglerFilesApi);
                        await filesDispatcher.Execute(action).ConfigureAwait(false);
                    }
                        break;
                    case SmugglerMode.Counter:
                    {
                        try
                        {
                            counterOptionSet.Parse(args);
                        }
                        catch (Exception e)
                        {
                            PrintUsageAndExit(e);
                        }

                        switch (action)
                        {
                            case SmugglerAction.Export:
                                smugglerCounterApi.Options.Source.Url = url;
                                smugglerCounterApi.Options.Source.CounterStoreId = counterStorageName;
                                break;
                            case SmugglerAction.Import:
                                smugglerCounterApi.Options.Destination.Url = url;
                                smugglerCounterApi.Options.Destination.CounterStoreId = counterStorageName;
                                break;
                            case SmugglerAction.Between:
                                smugglerCounterApi.Options.Source.Url = url;
                                smugglerCounterApi.Options.Destination.Url = url;
                                smugglerCounterApi.Options.Source.CounterStoreId = counterStorageName;
                                smugglerCounterApi.Options.Destination.CounterStoreId = counterStorageName2;
                                break;
                        }

                        smugglerCounterApi.Options.BackupPath = args[2];

                        var countersDispatcher = new SmugglerCounterOperationDispatcher(smugglerCounterApi.Options);
                        await countersDispatcher.Execute(action).ConfigureAwait(false);
                    }
                        break;
                    default:
                        Console.WriteLine("Smuggler mode is not identified. You should use --database or --filesystem.");
                        Environment.Exit(-1);
                        break;
                }
            }
            catch (Exception e)
            {
                if (e is AggregateException)
                {
                    Console.WriteLine(e.InnerException != null ? e.InnerException.SimplifyError() : e.SimplifyError());
                }
                else if (e is OperationVetoedException)
                {
                    Console.WriteLine("You are trying to import into database with versioning bundle enabled, use the flag: disable - versioning - during - import in the command line parameters\r\n");
                }
                else
                {
                    var errorResponseException = e as ErrorResponseException;
                    Console.WriteLine(errorResponseException != null ?
                        String.Format("{0} \n\r {1}", errorResponseException.SimplifyError(), errorResponseException.Response) : e.Message);
                }

                Environment.Exit(-1);
            }
        }

        private void ValidateDatabaseParameters(SmugglerDatabaseApi api, SmugglerAction action)
        {
            if (allowOperationOnSystemDatabase)
                return;

                if (string.IsNullOrEmpty(api.Options.Source.DefaultDatabase))
                {
                    throw new OptionException("--database parameter must be specified or pass --system-database", "database");
                }

                if (action == SmugglerAction.Between && string.IsNullOrEmpty(api.Options.Destination.DefaultDatabase))
                {
                    throw new OptionException("--database2 parameter must be specified or pass --system-database", "database2");
                }
            }

        private void PrintUsageAndExit(Exception e)
        {
            string message = e.Message;
            if (e is AggregateException)
                message = e.SimplifyError();

            ConsoleHelper.WriteLineWithColor(ConsoleColor.Red, message);
            PrintUsage();
            ConsoleHelper.WriteLineWithColor(ConsoleColor.Red, message);
            Environment.Exit(-1);
        }

        private void PrintUsageAndExit(int exitCode)
        {
            PrintUsage();
            Environment.Exit(exitCode);
        }

        private void PrintUsage()
        {
            ConsoleHelper.WriteLineWithColor(ConsoleColor.DarkMagenta, @"
Smuggler Import/Export utility for RavenDB
----------------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------------", SystemTime.UtcNow.Year);

            Console.WriteLine(@"
Usage:
  - Import the dump.raven file to the 'Northwind' database found on specified server:
    Raven.Smuggler in http://localhost:8080/ dump.raven --database=Northwind
  - Export 'Northwind' database from specified server to the dump.raven file:
    Raven.Smuggler out http://localhost:8080/ dump.raven --database=Northwind
  - Export from 'sourceDB' to 'targetDB' between two different servers:
    Raven.Smuggler between http://localhost:8080/  http://localhost:8081/ --database=sourceDB --database2=targetDB
  - Import a file system dump.ravenfs file to the MyFiles filesystem of the specified RavenDB instance:
    Raven.Smuggler in http://localhost:8080/ dump.ravenfs --filesystem=MyFiles
  - Export from MyFiles file system of the specified RavenDB instance to the dump.ravenfs file:
    Raven.Smuggler out http://localhost:8080/ dump.ravenfs --filesystem=MyFiles
  - Export from MyFiles1 to MyFiles2 on a different RavenDB instance:
    Raven.Smuggler between http://localhost:8080/ http://localhost:8081/ --filesystem=sourceDB --filesystem2=targetDB
    

Command line options:");

            switch (mode)
            {
                case SmugglerMode.Database:
                    selectionDispatching.WriteOptionDescriptions(Console.Out);
                    databaseOptionSet.WriteOptionDescriptions(Console.Out);
                    break;
                case SmugglerMode.Filesystem:
                    selectionDispatching.WriteOptionDescriptions(Console.Out);
                    filesystemOptionSet.WriteOptionDescriptions(Console.Out);
                    break;
                case SmugglerMode.Counter:
                    selectionDispatching.WriteOptionDescriptions(Console.Out);
                    counterOptionSet.WriteOptionDescriptions(Console.Out);
                    break;
                default:
                    selectionDispatching.WriteOptionDescriptions(Console.Out);
                    databaseOptionSet.WriteOptionDescriptions(Console.Out);
                    filesystemOptionSet.WriteOptionDescriptions(Console.Out);
                    counterOptionSet.WriteOptionDescriptions(Console.Out);
                    break;
            }

            Console.WriteLine();
        }
    }
}
