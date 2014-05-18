// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1562.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;

namespace Raven.Tests.Issues
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Transactions;

    using Raven.Abstractions.Data;
    using Raven.Abstractions.Replication;
    using Raven.Client;
    using Raven.Client.Connection;
    using Raven.Client.Indexes;
    using Raven.Database.Extensions;
    using Raven.Imports.Newtonsoft.Json;
    using Raven.Json.Linq;

    using Xunit;

    public class RavenDB_1562 : ReplicationBase
    {
        public IDocumentStore PrimaryDocumentStore;
        public IDocumentStore SecondaryDocumentStore;

        private const string dbName = "replication.deletebyindex";
        private string primaryDbName = dbName + "1";
        private string secondaryDbName = dbName + "2";

        public RavenDB_1562()
        {
            IOExtensions.DeleteDirectory("Databases/" + primaryDbName);
            IOExtensions.DeleteDirectory("Databases/" + secondaryDbName);

            PrimaryDocumentStore = CreateStore();
            SecondaryDocumentStore = CreateStore();
        }

        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.RunInMemory = false;
            configuration.DefaultStorageTypeName = GetDefaultStorageType("esent");
        }

        public override void Dispose()
        {
            if (PrimaryDocumentStore != null)
                PrimaryDocumentStore.Dispose();

            if (SecondaryDocumentStore != null)
                SecondaryDocumentStore.Dispose();

            base.Dispose();
        }

        [Fact]
        public void ReplicationDeleteByIndexRepro()
        {
            foreach (var server in servers)
            {
                EnsureDtcIsSupported(server);
            }

            PrimaryDocumentStore.Initialize();
            SecondaryDocumentStore.Initialize();

            SetupDB(SecondaryDocumentStore, secondaryDbName, false, null);
            SetupDB(PrimaryDocumentStore, primaryDbName, true, secondaryDbName);

            Create_DeleteByIndex_CreateWithTransactionScope_ReplicationToSecondaryFailsWithNonCurrentEtag(PrimaryDocumentStore, SecondaryDocumentStore, primaryDbName, secondaryDbName);
        }

        private void Create_DeleteByIndex_CreateWithTransactionScope_ReplicationToSecondaryFailsWithNonCurrentEtag(IDocumentStore primaryStore, IDocumentStore secondaryStore, string primaryDbName, string secondaryDbName)
        {
            var docId = "create-deletebyindex-createwithtransactionscope";

            Assert.Null(primaryStore.DatabaseCommands.ForDatabase(primaryDbName).Get(docId));
            Assert.Null(secondaryStore.DatabaseCommands.ForDatabase(secondaryDbName).Get(docId));

            using (var session = primaryStore.OpenSession(primaryDbName))
            {
                var doc = new Doc(docId, "first primaryStore");

                session.Store(doc, doc.Id);
                session.SaveChanges();
            }

            WaitForIndexing(primaryStore, primaryDbName);
            WaitForReplication(secondaryStore, docId, secondaryDbName);

            var databaseCommands = primaryStore.DatabaseCommands.ForDatabase(primaryDbName);

            using (var session = primaryStore.OpenSession(primaryDbName))
            {
                var deleteExistingDocsQuery = session.Advanced
                        .DocumentQuery<Doc, DocsIndex>()
                        .WhereIn("Id", new string[] { docId });

                var deleteExistingDocsIndexQuery = new IndexQuery { Query = deleteExistingDocsQuery.ToString() };

                var deleteByIndex = databaseCommands.DeleteByIndex(new DocsIndex().IndexName, deleteExistingDocsIndexQuery, false);
                var array = deleteByIndex.WaitForCompletion() as RavenJArray;

                Assert.Equal(1, array.Length);
                Assert.Equal(docId, array[0].Value<string>("Document"));
                Assert.Equal(true, array[0].Value<bool>("Deleted"));
            }

            WaitForReplication(secondaryStore, session => session.Load<Doc>(docId) == null, secondaryDbName);

            using (var transaction = new TransactionScope())
            {
                using (var session = primaryStore.OpenSession(primaryDbName))
                {
                    // insert with same id again
                    var docAgain = new Doc(docId, "second primaryStore with transaction scope");

                    session.Store(docAgain, docAgain.Id);

                    session.SaveChanges();

                    transaction.Complete();
                }
            }

            WaitForDocument(primaryStore.DatabaseCommands.ForDatabase(primaryDbName), docId);

            WaitForDocument(secondaryStore.DatabaseCommands.ForDatabase(secondaryDbName), docId);

            using (var session = secondaryStore.OpenSession(secondaryDbName))
            {
                Assert.Equal("second primaryStore with transaction scope", session.Load<Doc>(docId).Description);
            }
        }

        private void SetupDB(IDocumentStore store, string dbName, bool addReplicationTarget, string targetDbName)
        {
            DeleteDatabase(store, dbName, true);

            var databaseDocument = new DatabaseDocument
            {
                Id = dbName,
                Settings =
				{
					{"Raven/ActiveBundles", "Replication"},
					{"Raven/DataDir", "~\\Databases\\" + dbName}
				}
            };

            store.DatabaseCommands.EnsureDatabaseDocumentExists(databaseDocument);

            if (addReplicationTarget)
            {
                var replicationDestination = new Raven.Abstractions.Replication.ReplicationDestination()
                {
                    Url = SecondaryDocumentStore.Url,
                    TransitiveReplicationBehavior = TransitiveReplicationOptions.None,
                    Database = targetDbName
                };

                var newReplicationDocument = new Raven.Abstractions.Replication.ReplicationDocument()
                {
                    Destinations = new List<ReplicationDestination>()
					{
						replicationDestination
					}
                };

                using (var session = store.OpenSession(dbName))
                {
                    session.Store(newReplicationDocument, @"Raven/Replication/Destinations");

                    session.SaveChanges();
                }
            }

            new RavenDocumentsByEntityName().Execute(
                store.DatabaseCommands.ForDatabase(dbName),
                store.Conventions
            );

            new DocsIndex().Execute(
                store.DatabaseCommands.ForDatabase(dbName),
                store.Conventions
            );

        }

        public static void DeleteDatabase(IDocumentStore store, string name, bool hardDelete = false)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException("name");

            var databaseCommands = store.DatabaseCommands;
            var relativeUrl = "/admin/databases/" + name;

            if (hardDelete)
                relativeUrl += "?hard-delete=true";

            var serverClient = databaseCommands.ForSystemDatabase() as ServerClient;

			var httpJsonRequest = serverClient.CreateRequest(relativeUrl, "DELETE");
            httpJsonRequest.ExecuteRequest();
        }

    }

    public class Doc
    {
        public string Id { get; set; }

        public string Description { get; set; }

        public Doc(string id, string description)
        {
            Id = id;
            Description = description;
        }
    }

    public class DocsIndex : AbstractIndexCreationTask<Doc>
    {

        public DocsIndex()
        {
            Map = docs => from doc in docs
                          select new
                          {
                              Id = doc.Id,
                          };
        }

        public override string IndexName
        {
            get
            {
                return "Docs";
            }
        }
    }

    public static class MultiTenancyExtensions
    {
        public static void EnsureDatabaseDocumentExists(this IDatabaseCommands self, DatabaseDocument databaseDocument)
        {
            var serverClient = self.ForSystemDatabase() as ServerClient;

            if (serverClient == null)
            {
                throw new InvalidOperationException("Multiple databases are not supported in the embedded API currently");
            }

            serverClient.ForceReadFromMaster();

            if (serverClient.Get("Raven/Databases/" + Uri.EscapeDataString(databaseDocument.Id)) != null)
            {
                return;
            }

            if (databaseDocument.Settings.ContainsKey("Raven/DataDir") == false)
            {
                throw new InvalidOperationException("The Raven/DataDir setting is mandatory");
            }

            var doc = RavenJObject.FromObject(databaseDocument);
            doc.Remove("Id");

			var req = serverClient.CreateRequest("/admin/databases/" + Uri.EscapeDataString(databaseDocument.Id), "PUT");
            req.WriteAsync(doc.ToString(Formatting.Indented)).Wait();
            req.ExecuteRequest();

        }
    }
}