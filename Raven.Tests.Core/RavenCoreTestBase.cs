// -----------------------------------------------------------------------
//  <copyright file="RavenCoreTestBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;using Raven.Abstractions.Data;using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Extensions;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Tests.Core.Auth;
using Xunit;
using Raven.Server;
using Raven.Database;
using Raven.Database.Server;
using Authentication = Raven.Database.Server.Security.Authentication;

namespace Raven.Tests.Core
{
	public class RavenCoreTestBase : IUseFixture<TestServerFixture>, IDisposable
	{
		private readonly List<string> createdDbs = new List<string>();
		protected readonly List<DocumentStore> createdStores = new List<DocumentStore>();

		protected RavenDbServer Server { get; private set; }

		public void SetFixture(TestServerFixture coreTestFixture)
		{
			Server = coreTestFixture.Server;
		}

		protected virtual DocumentStore GetDocumentStore([CallerMemberName] string databaseName = null, string dbSuffixIdentifier = null, 
			Action<DatabaseDocument> modifyDatabaseDocument = null)
		{
			var serverClient = (ServerClient)Server.DocumentStore.DatabaseCommands.ForSystemDatabase();

			serverClient.ForceReadFromMaster();

			if (dbSuffixIdentifier != null)
				databaseName = string.Format("{0}_{1}", databaseName, dbSuffixIdentifier);

			var doc = MultiDatabase.CreateDatabaseDocument(databaseName);

			if (serverClient.Get(doc.Id) != null)
				throw new InvalidOperationException(string.Format("Database '{0}' already exists", databaseName));

			if (modifyDatabaseDocument != null)
				modifyDatabaseDocument(doc);

			serverClient.GlobalAdmin.CreateDatabase(doc);

			createdDbs.Add(databaseName);

			var documentStore = new DocumentStore
			{
				HttpMessageHandler = Server.DocumentStore.HttpMessageHandler,
				Url = Server.SystemDatabase.ServerUrl,
				DefaultDatabase = databaseName
			};
			documentStore.Initialize();

			createdStores.Add(documentStore);

			return documentStore;
		}

        public static void WaitForUserToContinueTheTest(DocumentStore documentStore, bool debug = true, int port = 8079)
        {
            if (debug && Debugger.IsAttached == false)
                return;

            OwinHttpServer server = null;
            string url = documentStore.Url;
            using (server)
            {
                var databaseNameEncoded = Uri.EscapeDataString(documentStore.DefaultDatabase ?? Constants.SystemDatabase);
                var documentsPage = url + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true";

                Process.Start(documentsPage); // start the server

                do
                {
                    Thread.Sleep(100);
                } while (documentStore.DatabaseCommands.Head("Debug/Done") == null && (debug == false || Debugger.IsAttached));
            }
        }


		public static void WaitForIndexing(DocumentStore store, string db = null, TimeSpan? timeout = null)
		{
			var databaseCommands = store.DatabaseCommands;
			if (db != null)
				databaseCommands = databaseCommands.ForDatabase(db);
			var spinUntil = SpinWait.SpinUntil(() => databaseCommands.GetStatistics().StaleIndexes.Length == 0, timeout ?? (Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(20)));

			if (spinUntil == false)
			{
				var statistics = databaseCommands.GetStatistics();
				var stats = RavenJObject.FromObject(statistics).ToString(Formatting.Indented);
				throw new TimeoutException("The indexes stayed stale for more than " + timeout.Value + Environment.NewLine + stats);
			}
		}

		protected void WaitForBackup(IDatabaseCommands commands, bool checkError)
		{
			WaitForBackup(commands.Get, checkError);
		}

		private void WaitForBackup(Func<string, JsonDocument> getDocument, bool checkError)
		{
			var done = SpinWait.SpinUntil(() =>
			{
				// We expect to get the doc from database that we tried to backup
				var jsonDocument = getDocument(BackupStatus.RavenBackupStatusDocumentKey);
				if (jsonDocument == null)
					return false;

				var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
				if (backupStatus.IsRunning == false)
				{
					if (checkError)
					{
						var firstOrDefault =
							backupStatus.Messages.FirstOrDefault(x => x.Severity == BackupStatus.BackupMessageSeverity.Error);
						if (firstOrDefault != null)
							Assert.True(false, string.Format("{0}\n\nDetails: {1}", firstOrDefault.Message, firstOrDefault.Details));
					}

					return true;
				}
				return false;
			}, Debugger.IsAttached ? TimeSpan.FromMinutes(120) : TimeSpan.FromMinutes(15));
			Assert.True(done);
		}

        public static void WaitForRestore(IDatabaseCommands databaseCommands)
        {
            var systemDatabaseCommands = databaseCommands.ForSystemDatabase();

            var failureMessages = new[]
			                      {
				                      "Esent Restore: Failure! Could not restore database!", 
									  "Error: Restore Canceled", 
									  "Restore Operation: Failure! Could not restore database!"
			                      };

            var restoreFinishMessages = new[]
			                            {
				                            "The new database was created", 
											"Esent Restore: Restore Complete", 
											"Restore ended but could not create the datebase document, in order to access the data create a database with the appropriate name",
			                            };

            var done = SpinWait.SpinUntil(() =>
            {
                var doc = systemDatabaseCommands.Get(RestoreStatus.RavenRestoreStatusDocumentKey);

                if (doc == null)
                    return false;

                var status = doc.DataAsJson.Deserialize<RestoreStatus>(new DocumentConvention());

                if (failureMessages.Any(status.Messages.Contains))
                    throw new InvalidOperationException("Restore failure: " + status.Messages.Aggregate(string.Empty, (output, message) => output + (message + Environment.NewLine)));

                return restoreFinishMessages.Any(status.Messages.Contains);
            }, TimeSpan.FromMinutes(1));

            Assert.True(done);
        }

        public static void WaitForDocument(IDatabaseCommands databaseCommands, string id)
        {
            var done = SpinWait.SpinUntil(() =>
            {
                var doc = databaseCommands.Get(id);
                return doc != null;
            }, TimeSpan.FromMinutes(1));

            Assert.True(done);
        }

		public virtual void Dispose()
		{
			Authentication.Disable();
			foreach (var store in createdStores)
			{
				store.Dispose();
			}

			foreach (var db in createdDbs)
			{
				Server.DocumentStore.DatabaseCommands.GlobalAdmin.DeleteDatabase(db, hardDelete: true);
			}
		}

        public static IEnumerable<object[]> InsertOptions
        {
            get
            {
                yield return new[] { new BulkInsertOptions { Format = BulkInsertFormat.Bson, Compression = BulkInsertCompression.GZip } };
                yield return new[] { new BulkInsertOptions { Format = BulkInsertFormat.Json } };
                yield return new[] { new BulkInsertOptions { Compression = BulkInsertCompression.None } };
            }
        }
	}
}