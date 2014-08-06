// -----------------------------------------------------------------------
//  <copyright file="Crud.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Xunit;
using Raven.Tests.Core.Utils.Entities;
using Raven.Abstractions.Data;
using System.Collections.Generic;
using Raven.Tests.Core.Utils.Indexes;
using Raven.Abstractions.Indexing;

namespace Raven.Tests.Core.Commands
{
    public class Documents : RavenCoreTestBase
    {
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

                var metadata = await store.AsyncDatabaseCommands.HeadAsync("companies/1");
                RavenJToken value = null;
                Assert.NotNull(metadata);
                Assert.True(metadata.Metadata.TryGetValue("SomeMetadataKey", out value));
                Assert.Equal("SomeMetadataValue", value);

                await store.AsyncDatabaseCommands.PatchAsync(
                    "companies/1",
                    new[]
                        {
                            new PatchRequest 
                                {
                                    Type = PatchCommandType.Add,
                                    Name = "NewArray",
                                    Value = "NewValue"
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Copy,
                                    Name = "Name",
                                    Value = "CopiedName"
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Inc,
                                    Name = "Phone",
                                    Value = -1
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Insert,
                                    Name = "Contacts",
                                    Position = 1,
                                    Value = RavenJObject.FromObject( new Contact { FirstName = "TestFirstName" } )
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Modify,
                                    Name = "Contacts",
                                    Position = 0,
                                    Nested = new[]
                                    {
                                        new PatchRequest
                                        {
                                            Type = PatchCommandType.Set,
                                            Name = "FirstName",
                                            Value = "SomeFirstName"
                                        }
                                    }
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Rename,
                                    Name = "Address2",
                                    Value = "Renamed"
                                },
                            new PatchRequest
                                {
                                    Type = PatchCommandType.Unset,
                                    Name = "Address1"
                                }
                        },
                    null);

                var item1 = await store.AsyncDatabaseCommands.GetAsync("companies/1");
                Assert.NotNull(item1);
                Assert.Equal("NewValue", item1.DataAsJson.Value<RavenJArray>("NewArray")[0]);
                Assert.Equal("testname", item1.DataAsJson.Value<string>("CopiedName"));
                Assert.Equal(0, item1.DataAsJson.Value<int>("Phone"));
                Assert.Equal("TestFirstName", item1.DataAsJson.Value<RavenJArray>("Contacts")[1].Value<string>("FirstName"));
                Assert.Equal("SomeFirstName", item1.DataAsJson.Value<RavenJArray>("Contacts")[0].Value<string>("FirstName"));
                Assert.Null(item1.DataAsJson.Value<string>("Address1"));
                Assert.Null(item1.DataAsJson.Value<string>("Address2"));
                Assert.Equal("Address2", item1.DataAsJson.Value<string>("Renamed"));

                await store.AsyncDatabaseCommands.DeleteAsync("companies/1", putResult.ETag);
                Assert.Null(await store.AsyncDatabaseCommands.GetAsync("companies/1"));

                await store.AsyncDatabaseCommands.DeleteDocumentAsync("users/2");
                Assert.Null(await store.AsyncDatabaseCommands.GetAsync("users/2"));
            }
        }

        [Fact]
        public async Task CanDeleteAndUpdateDocumentByIndex()
        {
            var usersByNameIndex = new Users_ByName();
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.PutIndex("MyIndex", new IndexDefinition
                {
                    Map = "from doc in docs select new { Name }"
                });

                store.DatabaseCommands.Put("items/1", null, RavenJObject.FromObject(new
                {
                    Name = "testname"
                }), new RavenJObject());
                WaitForIndexing(store);

                store.DatabaseCommands.UpdateByIndex("MyIndex", new IndexQuery { Query = "" }, new[] 
                {
                    new PatchRequest 
                    {
                        Type = PatchCommandType.Set,
                        Name = "NewName",
                        Value = "NewValue"
                    }
                },
                false).WaitForCompletion();

                var document = await store.AsyncDatabaseCommands.GetAsync("items/1");
                Assert.Equal("NewValue", document.DataAsJson.Value<string>("NewName"));

                store.DatabaseCommands.DeleteByIndex("MyIndex", new IndexQuery { Query = "" }, false).WaitForCompletion();
                var documents = store.DatabaseCommands.GetDocuments(0, 25);
                Assert.Equal(0, documents.Length);
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

                var documents = await store.AsyncDatabaseCommands.StartsWithAsync("Companies", null, 0, 25);
                Assert.Equal(1, documents.Length);
            }
        }

        [Fact]
        public void Replacing_Value()
        {
            const string oldTagName = "old";
            using (var store = GetDocumentStore())
            {

                store.DatabaseCommands.PutIndex("MyIndex", new IndexDefinition
                {
                    Map = "from doc in docs from note in doc.Comment.Notes select new { note}"
                });

                store.DatabaseCommands.Put("items/1", null, RavenJObject.FromObject(new
                {
                    Comment = new
                    {
                        Notes = new[] { "old", "item" }
                    }
                }), new RavenJObject());
                WaitForIndexing(store);

                store.DatabaseCommands.UpdateByIndex("MyIndex",
                   new IndexQuery
                   {
                       Query = "note:" + oldTagName
                   },
                   new[]
				   {
					   new PatchRequest
					   {
						   Name = "Comment",
						   Type = PatchCommandType.Modify,
						   AllPositions = true,
						   Nested = new[]
						   {
							   new PatchRequest
							   {
								   Type = PatchCommandType.Remove,
								   Name = "Notes",
								   Value = oldTagName
							   },
							   new PatchRequest
							   {
								   Type = PatchCommandType.Add,
								   Name = "Notes",
								   Value = "new"
							   }
						   }
					   }
				   },
                   false
               ).WaitForCompletion();

                Assert.Equal("{\"Comment\":{\"Notes\":[\"item\",\"new\"]}}", store.DatabaseCommands.Get("items/1").DataAsJson.ToString(Formatting.None));
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
