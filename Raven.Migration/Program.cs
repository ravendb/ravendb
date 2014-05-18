using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using NDesk.Options;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Migration.MigrationTasks;

namespace Raven.Migration
{
	public class Program
	{
		private readonly RavenConnectionStringOptions connectionStringOptions = new RavenConnectionStringOptions { Credentials = new NetworkCredential() };
		private readonly RavenConnectionStringOptions connectionStringOptions2 = new RavenConnectionStringOptions { Credentials = new NetworkCredential() };
		private readonly OptionSet optionSet;
		private string fileSystemName;
		private bool use2NdConnection;
		private bool deleteCopiedAttachments = false;

		public Program()
		{
			optionSet = new OptionSet
		    {
			    {
				    "filesystemserver:", "The url of the RavenDB instance where attachments will be copied to the specified file system",
				    value =>
				    {
					    use2NdConnection = true;
					    connectionStringOptions2.Url = value;
				    }
			    },
			    {"d|database:", "The database to operate on. If no specified, the operations will be on the default database.", value => connectionStringOptions.DefaultDatabase = value},
			    {"fs|filesystem:", "The file system to export to.", value => fileSystemName = value},
			    {"u|user|username:", "The username to use when the database requires the client to authenticate.", value => ((NetworkCredential) connectionStringOptions.Credentials).UserName = value},
				{"u2|user2|username2:", "The username to use when the database requires the client to authenticate. This parameter is used only in the between operation.", value => ((NetworkCredential) connectionStringOptions2.Credentials).UserName = value},
			    {"p|pass|password:", "The password to use when the database requires the client to authenticate.", value => ((NetworkCredential) connectionStringOptions.Credentials).Password = value},
				{"p2|pass2|password2:", "The password to use when the database requires the client to authenticate. This parameter is used only in the between operation.", value => ((NetworkCredential) connectionStringOptions2.Credentials).Password = value},
			    {"domain:", "The domain to use when the database requires the client to authenticate.", value => ((NetworkCredential) connectionStringOptions.Credentials).Domain = value},
				{"domain2:", "The domain to use when the database requires the client to authenticate. This parameter is used only in the between operation.", value => ((NetworkCredential) connectionStringOptions2.Credentials).Domain = value},
			    {"key|api-key|apikey:", "The API-key to use, when using OAuth.", value => connectionStringOptions.ApiKey = value},
				{"key2|api-key2|apikey2:", "The API-key to use, when using OAuth. This parameter is used only in the between operation.", value => connectionStringOptions2.ApiKey = value},
			    {"h|?|help", v => PrintUsageAndExit(0)},
				{"deletecopiedattachments", v => deleteCopiedAttachments = true},
		    };
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

			connectionStringOptions.Url = url;

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
				new CopyAttachmentsToFileSystem(connectionStringOptions, use2NdConnection ? connectionStringOptions2 : null, fileSystemName, deleteCopiedAttachments).Execute();
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
						Console.WriteLine("Error: {0} {1}", e.Message, connectionStringOptions.Url + (use2NdConnection ? " => " + connectionStringOptions2.Url : ""));
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
----------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------
Usage:
	- Move all attachments from MyDatabase database to the specified MyFileSystem file system within the same RavenDB instance:
		Raven.Migration http://localhost:8080/ --database=MyDatabase --filesystem=MyFileSystem
	- Move all attachments from MyDatabase database to the specified MyFileSystem file system on the different RavenDB instance:
		Raven.Migration http://localhost:8080/ --database=MyDatabase --filesystemserver=http://localhost:8081/ --filesystem=MyFileSystem

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
