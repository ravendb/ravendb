// -----------------------------------------------------------------------
//  <copyright file="Crud.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Json.Linq;
using Xunit;
using Raven.Tests.Core.Utils.Entities;
using Raven.Abstractions.Data;
using System.Collections.Generic;

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
                    new RavenJObject());
                Assert.NotNull(await store.AsyncDatabaseCommands.GetAsync("companies/1"));

                await store.AsyncDatabaseCommands.PutAsync("users/2", null, RavenJObject.FromObject(new User { Name = "testname2" }), new RavenJObject());
                Assert.NotNull(await store.AsyncDatabaseCommands.GetAsync("users/2"));

                var documents = await store.AsyncDatabaseCommands.GetDocumentsAsync(0, 25);
                Assert.Equal(2, documents.Length);

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
        public async Task CanGetMultipleDocumentsWithIncludes()
        {
        }
    }
}
