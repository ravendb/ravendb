// -----------------------------------------------------------------------
//  <copyright file="Crud.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using FastTests;
using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Client.Indexing;
using Raven.Json.Linq;

using SlowTests.Core.Utils.Indexes;

using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;
using Contact = SlowTests.Core.Utils.Entities.Contact;
using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Core.Commands
{
    public class Documents : RavenTestBase
    {
        [Fact]
        public void CanCancelPutDocument()
        {
            var random = new Random();
            var largeArray = new byte[1024 * 1024 * 2];
            //2mb - document large enough for PUT to take a while, so
            //we will be able to cancel the operation BEFORE the PUT completes
            random.NextBytes(largeArray);
            var largeDocument = new { Data = largeArray };

            var cts = new CancellationTokenSource();
            using (var store = GetDocumentStore())
            {
                var ravenJObject = RavenJObject.FromObject(largeDocument);
                cts.Cancel();
                var putTask = store.AsyncDatabaseCommands.PutAsync("test/1", null, ravenJObject, new RavenJObject(), cts.Token);
                Assert.True(putTask.IsCanceled);
            }
        }

        [Fact]
        public async Task CanPutGetUpdateAndDeleteDocument()
        {
            using (var store = GetDocumentStore())
            {
                var putResult = await store.AsyncDatabaseCommands.PutAsync(
                    "companies/1",
                    null,
                    RavenJObject.FromObject(new Company
                    {
                        Name = "testname",
                        Phone = 1,
                        Contacts = new List<Contact> { new Contact { }, new Contact { } },
                        Address1 = "To be removed.",
                        Address2 = "Address2"
                    }),
                    RavenJObject.FromObject(new
                    {
                        SomeMetadataKey = "SomeMetadataValue"
                    }));
                Assert.NotNull(await store.AsyncDatabaseCommands.GetAsync("companies/1"));

                await store.AsyncDatabaseCommands.PutAsync("users/2", null, RavenJObject.FromObject(new User { Name = "testname2" }), new RavenJObject());
                Assert.NotNull(await store.AsyncDatabaseCommands.GetAsync("users/2"));

                var documents = await store.AsyncDatabaseCommands.GetDocumentsAsync(0, 25);
                Assert.Equal(2, documents.Length);

                var etag = await store.AsyncDatabaseCommands.HeadAsync("companies/1");
                RavenJToken value = null;
                Assert.NotNull(etag);

                var document = await store.AsyncDatabaseCommands.GetAsync("companies/1", metadataOnly: true);
                Assert.NotNull(document.DataAsJson);
                Assert.Equal(0, document.DataAsJson.Count);
                Assert.True(document.Metadata.TryGetValue("SomeMetadataKey", out value));
                Assert.Equal("SomeMetadataValue", value);

                await store.AsyncDatabaseCommands.DeleteAsync("companies/1", putResult.ETag);
                Assert.Null(await store.AsyncDatabaseCommands.GetAsync("companies/1"));

                await store.AsyncDatabaseCommands.DeleteAsync("users/2", null);
                Assert.Null(await store.AsyncDatabaseCommands.GetAsync("users/2"));
            }
        }

        [Fact]
        public async Task CanDeleteAndUpdateDocumentByIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.PutIndex("MyIndex", new IndexDefinition
                {
                    Maps = { "from doc in docs.Items select new { doc.Name }" }
                });

                store.DatabaseCommands.Put("items/1", null, RavenJObject.FromObject(new
                {
                    Name = "testname"
                }), new RavenJObject
                {
                    {"Raven-Entity-Name",  "Items"}
                });
                WaitForIndexing(store);

                store.DatabaseCommands.UpdateByIndex("MyIndex", new IndexQuery { Query = "" }, new PatchRequest { Script = "this.NewName = 'NewValue';" }, null).WaitForCompletion(TimeSpan.FromSeconds(15));

                var document = await store.AsyncDatabaseCommands.GetAsync("items/1");
                Assert.Equal("NewValue", document.DataAsJson.Value<string>("NewName"));
                WaitForIndexing(store);
                store.DatabaseCommands.DeleteByIndex("MyIndex", new IndexQuery { Query = "" }, null).WaitForCompletion(TimeSpan.FromSeconds(15));
                var documents = store.DatabaseCommands.GetDocuments(0, 25);
                Assert.Equal(0, documents.Length);
            }
        }

        [Fact(Skip = "Missing feature: /docs/startsWith")]
        public async Task CanGetDocumentsWhoseIdStartsWithAPrefix()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company { Name = "Something with the desired prefix" });
                    await session.StoreAsync(new Contact { Surname = "Something without the desired prefix" });
                    await session.SaveChangesAsync();
                }

                var documents = await store.AsyncDatabaseCommands.StartsWithAsync("Companies", null, 0, 25);
                Assert.Equal(1, documents.Length);
            }
        }

        [Fact(Skip = "Missing feature: /docs/stream")]
        public void CanStreamDocs()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 200; i++)
                    {
                        session.Store(new User());
                    }
                    session.SaveChanges();
                }

                int count = 0;
                using (var reader = store.DatabaseCommands.StreamDocs(startsWith: "users/"))
                {
                    while (reader.MoveNext())
                    {
                        count++;
                    }
                }
                Assert.Equal(200, count);
            }
        }
    }
}
