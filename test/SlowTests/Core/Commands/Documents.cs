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
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
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
            var document = new { Name = "John" };

            var cts = new CancellationTokenSource();
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    cts.Cancel();
                    var putTask = commands.PutAsync("test/1", null, document, null, cts.Token);

                    try
                    {
                        try
                        {
                            putTask.Wait(TimeSpan.FromMinutes(1));
                        }
                        catch (AggregateException e) when (e.InnerException is OperationCanceledException)
                        {
                        }
                        Assert.True(putTask.IsCanceled);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Task was expected to be cacnelled, but was " + putTask.Status, e);
                    }
                }
            }
        }

        [Fact]
        public async Task CanPutGetUpdateAndDeleteDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    var putResult = await commands.PutAsync(
                        "companies/1",
                        null,
                        new Company
                        {
                            Name = "testname",
                            Phone = 1,
                            Contacts = new List<Contact> { new Contact { }, new Contact { } },
                            Address1 = "To be removed.",
                            Address2 = "Address2"
                        },
                        new Dictionary<string, object>
                        {
                            {"SomeMetadataKey", "SomeMetadataValue"}
                        });

                    Assert.NotNull(await commands.GetAsync("companies/1"));

                    await commands.PutAsync("users/2", null, new User { Name = "testname2" }, null);

                    Assert.NotNull(await commands.GetAsync("users/2"));

                    var documents = await commands.GetAsync(0, 25);
                    Assert.Equal(2, documents.Length);

                    var etag = await commands.HeadAsync("companies/1");
                    Assert.NotNull(etag);

                    var document = await commands.GetAsync("companies/1", metadataOnly: true);
                    Assert.Equal(1, document.BlittableJson.Count);

                    var metadata = document.BlittableJson.GetMetadata();
                    string someMetadataValue;
                    Assert.True(metadata.TryGet("SomeMetadataKey", out someMetadataValue));
                    Assert.Equal("SomeMetadataValue", someMetadataValue);

                    await commands.DeleteAsync("companies/1", putResult.ChangeVector);
                    Assert.Null(await commands.GetAsync("companies/1"));

                    await commands.DeleteAsync("users/2", null);
                    Assert.Null(await commands.GetAsync("users/2"));
                }
            }
        }

        [Fact]
        public async Task CanDeleteAndUpdateDocumentByIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs.Items select new { doc.Name }" },
                    Name = "MyIndex"
                }}));

                using (var commands = store.Commands())
                {
                    await commands.PutAsync("items/1", null, new { Name = "testname" }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "Items"}
                    });

                    WaitForIndexing(store);

                    var operation = store.Operations.Send(new PatchByQueryOperation(new IndexQuery { Query = "FROM INDEX 'MyIndex' UPDATE { this.NewName = 'NewValue'; } " }));
                    operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                    dynamic document = await commands.GetAsync("items/1");
                    Assert.Equal("NewValue", document.NewName.ToString());
                    WaitForIndexing(store);

                    operation = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery() { Query = $"FROM INDEX 'MyIndex'" }));
                    operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                    var documents = await commands.GetAsync(0, 25);
                    Assert.Equal(0, documents.Length);
                }
            }
        }

        [Fact]
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


                using (var session = store.OpenSession())
                {
                    var documents = session.Advanced.LoadStartingWith<dynamic>("Companies");
                    Assert.Equal(1, documents.Length);
                }
            }
        }

        [Fact]
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

                var count = 0;
                using (var session = store.OpenSession())
                {
                    using (var reader = session.Advanced.Stream<dynamic>(startsWith: "users/"))
                    {
                        while (reader.MoveNext())
                        {
                            count++;
                        }
                    }
                }

                Assert.Equal(200, count);
            }
        }
    }
}
