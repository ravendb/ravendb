// -----------------------------------------------------------------------
//  <copyright file="RavenCoreTestBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Extensions;
using System.Linq;

using Raven.Client;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Xunit;

using Raven.Server;
using Authentication = Raven.Database.Server.Security.Authentication;

namespace Raven.Tests.Core
{
    public abstract class RavenCoreTestBase : IUseFixture<TestServerFixture>, IDisposable
    {
        private readonly List<string> createdDbs = new List<string>();
        protected readonly List<DocumentStore> createdStores = new List<DocumentStore>();

        private readonly IDocumentStore serverDocumentStore;

        protected RavenDbServer Server { get; private set; }

        public void SetFixture(TestServerFixture fixture)
        {
            Server = fixture.Server;
        }

        protected virtual DocumentStore GetDocumentStore([CallerMemberName] string databaseName = null, string dbSuffixIdentifier = null,
            Action<DatabaseDocument> modifyDatabaseDocument = null)
        {
            var serverClient = (ServerClient)GetServerCommands().ForSystemDatabase();

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

            var documentStore = CreateDocumentStore(databaseName);

            createdStores.Add(documentStore);

            return documentStore;
        }

        public static void WaitForUserToContinueTheTest(DocumentStore documentStore, bool debug = true, int port = 8079)
        {
            if (debug && Debugger.IsAttached == false)
                return;

            string url = documentStore.Url;

            var databaseNameEncoded = Uri.EscapeDataString(documentStore.DefaultDatabase ?? Constants.SystemDatabase);
            var documentsPage = url + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true";

            Process.Start(documentsPage); // start the server

            do
            {
                Thread.Sleep(100);
            } while (documentStore.DatabaseCommands.Head("Debug/Done") == null && (debug == false || Debugger.IsAttached));
        }

        public static void WaitForIndexing(DocumentStore store, string db = null, TimeSpan? timeout = null)
        {
            var databaseCommands = store.DatabaseCommands;
            if (db != null)
                databaseCommands = databaseCommands.ForDatabase(db);
            var to = timeout ?? (Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(20));
            var spinUntil = SpinWait.SpinUntil(() => databaseCommands.GetStatistics().StaleIndexes.Length == 0, to);

            if (spinUntil == false)
            {
                var statistics = databaseCommands.GetStatistics();
                var stats = RavenJObject.FromObject(statistics).ToString(Formatting.Indented);
                throw new TimeoutException("The indexes stayed stale for more than " + to + Environment.NewLine + stats);
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
                GetServerCommands().GlobalAdmin.DeleteDatabase(db, hardDelete: true);
            }
        }

        private DocumentStore CreateDocumentStore(string databaseName)
        {
            var documentStore = new DocumentStore
            {
                HttpMessageHandlerFactory = Server.DocumentStore.HttpMessageHandlerFactory,
                Url = Server.SystemDatabase.ServerUrl,
                DefaultDatabase = databaseName
            };

            documentStore.Initialize();

            return documentStore;
        }

        private IDatabaseCommands GetServerCommands()
        {
            return Server.DocumentStore.DatabaseCommands;
        }
    }
}
