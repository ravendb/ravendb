// -----------------------------------------------------------------------
//  <copyright file="MigrationTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.FileSystem;

namespace Raven.Migration.MigrationTasks
{
	public abstract class MigrationTask
	{
		public abstract void Execute();

		protected DocumentStore CreateStore(RavenConnectionStringOptions options)
		{
			var s = new DocumentStore
			{
				Url = options.Url,
				ApiKey = options.ApiKey,
				Credentials = options.Credentials
			};

			s.Initialize();

			ValidateThatServerIsUpAndDatabaseExists(options, s);

			s.DefaultDatabase = options.DefaultDatabase;

			return s;
		}

		protected AsyncFilesServerClient CreateFileSystemClient(RavenConnectionStringOptions options, string fileSystemName)
		{
			var fsClient = new AsyncFilesServerClient(options.Url, fileSystemName, apiKey: options.ApiKey, credentials: options.Credentials);

			ValidateThatServerIsUpAndFileSystemExists(fsClient);

			return fsClient;
		}

		protected void ValidateThatServerIsUpAndDatabaseExists(RavenConnectionStringOptions options, DocumentStore s)
		{
			var shouldDispose = false;

			try
			{
				var commands = !string.IsNullOrEmpty(options.DefaultDatabase)
								   ? s.DatabaseCommands.ForDatabase(options.DefaultDatabase)
								   : s.DatabaseCommands;

				commands.GetStatistics(); // check if database exist
			}
			catch (Exception e)
			{
				shouldDispose = true;

				var responseException = e as ErrorResponseException;
				if (responseException != null && responseException.StatusCode == HttpStatusCode.ServiceUnavailable && responseException.Message.StartsWith("Could not find a database named"))
					throw new InvalidOperationException(
						string.Format(
							"Migration tool does not support database creation (database '{0}' on server '{1}' must exist before running this tool).",
							options.DefaultDatabase,
							s.Url), e);


				if (e.InnerException != null)
				{
					var webException = e.InnerException as WebException;
					if (webException != null)
					{
						throw new InvalidOperationException(string.Format("Migration tool encountered a connection problem: '{0}'.", webException.Message), webException);
					}
				}

				throw new InvalidOperationException(string.Format("Migration tool encountered a connection problem: '{0}'.", e.Message), e);
			}
			finally
			{
				if (shouldDispose)
					s.Dispose();
			}
		}

		protected void ValidateThatServerIsUpAndFileSystemExists(AsyncFilesServerClient fsClient)
		{
			var shouldDispose = false;

			try
			{
				var fileSystemStats = fsClient.GetStatisticsAsync().Result;
			}
			catch (Exception e)
			{
				shouldDispose = true;

				var responseException = e as ErrorResponseException;
				if (responseException != null && responseException.StatusCode == HttpStatusCode.ServiceUnavailable && responseException.Message.StartsWith("Could not find a file system named:"))
					throw new InvalidOperationException(
						string.Format(
							"Migration tool does not support file system creation (file system '{0}' on server '{1}' must exist before running this tool).",
							fsClient.FileSystem,
                            fsClient.ServerUrl), e);

				if (e.InnerException != null)
				{
					var webException = e.InnerException as WebException;
					if (webException != null)
					{
						throw new InvalidOperationException(string.Format("Migration tool encountered a connection problem: '{0}'.", webException.Message), webException);
					}
				}

				throw new InvalidOperationException(string.Format("Migration tool encountered a connection problem: '{0}'.", e.Message), e);
			}
			finally
			{
				if (shouldDispose)
					fsClient.Dispose();
			}
		}
	}
}