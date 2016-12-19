using System;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Linq;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Bundles.CascadeDelete;
using Raven.Bundles.Expiration;
using Raven.Client.Document;
using Raven.Database;
using Raven.Database.Bundles.Expiration;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.Expiration
{
    public class WithCascade : RavenTest
    {
        private readonly RavenDbServer ravenDbServer;
        private readonly DocumentStore documentStore;

        private readonly DocumentDatabase database;

        public WithCascade()
        {
            ravenDbServer = GetNewServer(databaseName: Constants.SystemDatabase, activeBundles: "DocumentExpiration;Cascade Delete", configureConfig: configuration =>
            {
                configuration.Catalog.Catalogs.Add(new AssemblyCatalog(typeof(CascadeDeleteTrigger).Assembly));
                configuration.Settings["Raven/Expiration/DeleteFrequencySeconds"] = "1";
            });
            documentStore = NewRemoteDocumentStore(ravenDbServer: ravenDbServer, databaseName: Constants.SystemDatabase);

            database = ravenDbServer.Server.GetDatabaseInternal(Constants.SystemDatabase).Result;
        }


        [Fact]
        public void CanDeleteAndCascadeAtTheSameTime()
        {
            documentStore.DatabaseCommands.PutAttachment("item", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
            using (var session = documentStore.OpenSession())
            {
                var doc = new { Id = "doc/1" };
                session.Store(doc);
                session.Advanced.GetMetadataFor(doc)["Raven-Expiration-Date"] = DateTime.Now.AddDays(-15);
                session.Advanced.GetMetadataFor(doc)[MetadataKeys.AttachmentsToCascadeDelete] = new RavenJArray(new[] { "item" });
                session.SaveChanges();
            }

            JsonDocument documentByKey = null;
            for (int i = 0; i < 50; i++)
            {
                database.TransactionalStorage.Batch(accessor =>
                {
                    documentByKey = accessor.Documents.DocumentByKey("doc/1");

                });
                if (documentByKey == null)
                    break;
                Thread.Sleep(100);
            }

            Assert.Null(documentByKey);

            database.TransactionalStorage.Batch(accessor => Assert.Null(accessor.Attachments.GetAttachment("item")));
        
        }

        [Fact]
        public void CanDeleteAndCascadeAtTheSameTimeDocuemnts()
        {
        //	documentStore.DatabaseCommands.Put("doc/1", new Etag(), new RavenJObject(), new RavenJObject());
            using (var session = documentStore.OpenSession())
            {
                var doc1 = new { Id = "doc/1" };
                var doc2 = new { Id = "doc/2" };
                session.Store(doc1);
                session.Store(doc2);
                session.Advanced.GetMetadataFor(doc1)["Raven-Expiration-Date"] = DateTime.Now.AddDays(-15);
                session.Advanced.GetMetadataFor(doc1)[MetadataKeys.DocumentsToCascadeDelete] = new RavenJArray(new[] { "doc/2" });
                session.SaveChanges();
            }

            JsonDocument documentByKey = null;
            for (int i = 0; i < 50; i++)
            {
                database.TransactionalStorage.Batch(accessor =>
                {
                    documentByKey = accessor.Documents.DocumentByKey("doc/1");

                });
                if (documentByKey == null)
                    break;
                Thread.Sleep(100);
            }

            Assert.Null(documentByKey);


            database.TransactionalStorage.Batch(accessor => Assert.Null(accessor.Documents.DocumentByKey("doc/2")));
        }

        [Fact]
        public void CanDeleteMultiChildrenWithCascade()
        {
            using (var session = documentStore.OpenSession())
            {
                var parent = new Foo();
                var child1 = new Foo();
                var child2 = new Foo();
                session.Store(parent, "parentId1");
                session.Store(child1, "childId1");
                session.Store(child2, "childId2");
                session.Advanced.GetMetadataFor(parent)["Raven-Cascade-Delete-Documents"] = RavenJToken.FromObject(new[] { "childId1", "childId2" });
                session.Advanced.GetMetadataFor(parent)["Raven-Expiration-Date"] = new RavenJValue(DateTime.UtcNow.AddSeconds(-4));
                session.SaveChanges();
            }

            WaitForIndexing(database);

            var expiredDocumentsCleaner = ravenDbServer.SystemDatabase.StartupTasks.OfType<ExpiredDocumentsCleaner>().First();
            while (expiredDocumentsCleaner.TimerCallback() == false)
            {
                Thread.Sleep(100);
            }

            using (var session = documentStore.OpenSession())
            {
                var list = session.Query<Foo>().ToList();
                Assert.Empty(list);
            }
        }

        public class Foo
        {

        }
    }
}
