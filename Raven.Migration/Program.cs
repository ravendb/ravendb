using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using NDesk.Options;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Migration.MigrationTasks;
using Raven.Smuggler.Helpers;

namespace Raven.Migration
{
    public class Program
    {
        private readonly RavenConnectionStringOptions dbConnectionStringOptions = new RavenConnectionStringOptions { Credentials = new NetworkCredential() };
        private readonly RavenConnectionStringOptions fsConnectionStringOptions = new RavenConnectionStringOptions { Credentials = new NetworkCredential() };
        private readonly OptionSet optionSet;
        private string fileSystemName;
        private bool use2NdConnection;
        private bool deleteCopiedAttachments = false;
        private int batchSize = 128;

        public Program()
        {
            optionSet = new OptionSet();
            optionSet.OnWarning += s => ConsoleHelper.WriteLineWithColor(ConsoleColor.Yellow, s);
            optionSet.Add("fs-server:", OptionCategory.None, "The url of the RavenDB instance where attachments will be copied to the specified file system", value =>
            {
                use2NdConnection = true;
                fsConnectionStringOptions.Url = value;
            });
            optionSet.Add("d|database:", OptionCategory.None, "The database to operate on. If no specified, the operations will be on the default database.", value => dbConnectionStringOptions.DefaultDatabase = value);
            optionSet.Add("fs|filesystem:", OptionCategory.None, "The file system to export to.", value => fileSystemName = value);
            optionSet.Add("u|user|username:", OptionCategory.None, "The username to use when the database requires the client to authenticate.", value => ((NetworkCredential)dbConnectionStringOptions.Credentials).UserName = value);
            optionSet.Add("u2|user2|username2:", OptionCategory.None, "The username to use when the file system requires the client to authenticate.", value =>
            {
                use2NdConnection = true;
                ((NetworkCredential)fsConnectionStringOptions.Credentials).UserName = value;
            });
            optionSet.Add("db-pass|db-password:", OptionCategory.None, "The password to use when the database requires the client to authenticate.", value => ((NetworkCredential)dbConnectionStringOptions.Credentials).Password = value);
            optionSet.Add("fs-pass|fs-password:", OptionCategory.None, "The password to use when the file system requires the client to authenticate.", value =>
            {
                use2NdConnection = true;
                ((NetworkCredential)fsConnectionStringOptions.Credentials).Password = value;
            });
            optionSet.Add("db-domain:", OptionCategory.None, "The domain to use when the database requires the client to authenticate.", value => ((NetworkCredential)dbConnectionStringOptions.Credentials).Domain = value);
            optionSet.Add("fs-domain:", OptionCategory.None, "The domain to use when the file system requires the client to authenticate.", value =>
            {
                use2NdConnection = true;
                ((NetworkCredential)fsConnectionStringOptions.Credentials).Domain = value;
            });
            optionSet.Add("db-key|db-api-key|db-apikey:", OptionCategory.None, "The API-key to use if the database requires OAuth authentication.", value => dbConnectionStringOptions.ApiKey = value);
            optionSet.Add("fs-key|fs-api-key|fs-apikey:", OptionCategory.None, "The API-key to use if the file system requires OAuth authentication.", value =>
            {
                use2NdConnection = true;
                fsConnectionStringOptions.ApiKey = value;
            });
            optionSet.Add("bs|batch-size:", OptionCategory.None, "Batch size for downloading attachments at once and uploading one-by-one to the file system. Default: 128.", value => batchSize = int.Parse(value));
            optionSet.Add("delete-copied-attachments", OptionCategory.None, "Delete an attachment after uploading it to the file system.", v => deleteCopiedAttachments = true);
            optionSet.Add("h|?|help", OptionCategory.Help, string.Empty, v => PrintUsageAndExit(0));
        }

        private void Parse(string[] args)
        {
            if (args.Length < 2)
                PrintUsageAndExit(-1);

            var url = args[0];
            if (url == null)
            {
                PrintUsageAndExit(-1);
                return;
            }

            dbConnectionStringOptions.Url = url;

            try
            {
                optionSet.Parse(args);
            }
            catch (Exception e)
            {
                PrintUsageAndExit(e);
            }

            // CopyAttachmentsToFileSystem is the only migration task right now

            if (string.IsNullOrEmpty(fileSystemName))
                PrintUsageAndExit(-1);

            try
            {
                new CopyAttachmentsToFileSystem(dbConnectionStringOptions, use2NdConnection ? fsConnectionStringOptions : dbConnectionStringOptions, fileSystemName, deleteCopiedAttachments, batchSize).Execute();
            }
            catch (Exception ex)
            {
                var exception = ex;
                var aggregateException = ex as AggregateException;

                if (aggregateException != null)
                {
                    exception = aggregateException.ExtractSingleInnerException();
                }

                var e = exception as WebException;
                if (e != null)
                {
                    if (e.Status == WebExceptionStatus.ConnectFailure)
                    {
                        Console.WriteLine("Error: {0} {1}", e.Message, dbConnectionStringOptions.Url + (use2NdConnection ? " => " + fsConnectionStringOptions.Url : ""));
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
Migration utility for RavenDB
----------------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------------
Usage:
    - Move all attachments from MyDatabase database to the specified MyFileSystem file system within the same RavenDB instance:
        Raven.Migration http://localhost:8080/ --database=MyDatabase --filesystem=MyFileSystem
    - Move all attachments from MyDatabase database to the specified MyFileSystem file system on the different RavenDB instance:
        Raven.Migration http://localhost:8080/ --database=MyDatabase --filesystem=MyFileSystem --fs-server=http://localhost:8081/

Command line options:", SystemTime.UtcNow.Year);

            optionSet.WriteOptionDescriptions(Console.Out);
            Console.WriteLine();

            Environment.Exit(exitCode);
        }

        static void Main(string[] args)
        {
            var program = new Program();
            program.Parse(args);
        }
    }
}
