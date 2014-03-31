// -----------------------------------------------------------------------
//  <copyright file="SmugglerBetweenTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Smuggler
{
    public class SmugglerBetweenTests : RavenTest
    {
        [Fact]
        public async Task ShouldWork()
        {
            using (var server1 = GetNewServer(port: 8079))
            using (var store1 = NewRemoteDocumentStore(ravenDbServer: server1, databaseName: "Database1"))
            {
				await new UsersIndex().ExecuteAsync(store1.AsyncDatabaseCommands, new DocumentConvention());
                await new UsersTransformer().ExecuteAsync(store1);
                using (var session = store1.OpenAsyncSession("Database1"))
                {
                    await session.StoreAsync(new User {Name = "Oren Eini"});
                    await session.StoreAsync(new User {Name = "Fitzchak Yitzchaki"});
                    await session.SaveChangesAsync();
                }
                await store1.AsyncDatabaseCommands.PutAttachmentAsync("ayende", null, new MemoryStream(new byte[] { 3 }), new RavenJObject());
                await store1.AsyncDatabaseCommands.PutAttachmentAsync("fitzchak", null, new MemoryStream(new byte[] { 2 }), new RavenJObject());

                using (var server2 = GetNewServer(port: 8078))
                {
	                await SmugglerOperation.Between(new SmugglerBetweenOptions
	                {
		                From = new RavenConnectionStringOptions {Url = "http://localhost:8079", DefaultDatabase = "Database1"},
		                To = new RavenConnectionStringOptions {Url = "http://localhost:8078", DefaultDatabase = "Database2"}
	                }, new SmugglerOptions());

                    using (var store2 = NewRemoteDocumentStore(ravenDbServer: server2, databaseName: "Database2"))
                    {
                        await AssertDatabaseHasIndex<UsersIndex>(store2);
                        await AssertDatabaseHasTransformer<UsersTransformer>(store2);

                        using (var session2 = store2.OpenAsyncSession("Database2"))
                        {
                            Assert.Equal(2, await session2.Query<User>().CountAsync());
                        }

                        var attachments = await store2.AsyncDatabaseCommands.GetAttachmentsAsync(Etag.Empty, 25);
                        Assert.Equal(2, attachments.Length);
                        Assert.Equal("ayende", attachments[0].Key);
                        Assert.Equal("fitzchak", attachments[1].Key);
                    }
                }
            }
        }

        [Fact]
        public async Task ShouldSupportIncremental()
        {
            using (var server1 = GetNewServer(port: 8079))
            using (var store1 = NewRemoteDocumentStore(ravenDbServer: server1, databaseName: "Database1"))
            {
                using (var session = store1.OpenAsyncSession("Database1"))
                {
                    await session.StoreAsync(new User {Name = "Oren Eini"});
                    await session.StoreAsync(new User {Name = "Fitzchak Yitzchaki"});
                    await session.SaveChangesAsync();
                }
                await store1.AsyncDatabaseCommands.PutAttachmentAsync("ayende", null, new MemoryStream(new byte[] {3}), new RavenJObject());
                await store1.AsyncDatabaseCommands.PutAttachmentAsync("fitzchak", null, new MemoryStream(new byte[] {2}), new RavenJObject());

                using (var server2 = GetNewServer(port: 8078))
                {
					await SmugglerOperation.Between(new SmugglerBetweenOptions
					{
						From = new RavenConnectionStringOptions { Url = "http://localhost:8079", DefaultDatabase = "Database1" },
						To = new RavenConnectionStringOptions { Url = "http://localhost:8078", DefaultDatabase = "Database2" }
					}, new SmugglerOptions
					{
						Incremental = true,
					});

                    using (var session = store1.OpenAsyncSession("Database1"))
                    {
                        var oren = await session.LoadAsync<User>("users/1");
                        oren.Name += " Changed";
                        await session.StoreAsync(new User {Name = "Daniel Dar"});
                        await session.SaveChangesAsync();
                    }
                    await store1.AsyncDatabaseCommands.PutAttachmentAsync("ayende", null, new MemoryStream(new byte[] {4}), new RavenJObject());
                    await store1.AsyncDatabaseCommands.PutAttachmentAsync("daniel", null, new MemoryStream(new byte[] {5}), new RavenJObject());

                    using (var store2 = NewRemoteDocumentStore(ravenDbServer: server2, databaseName: "Database2"))
                    {
                        using (var session2 = store2.OpenAsyncSession("Database2"))
                        {
                            var oren = await session2.LoadAsync<User>("users/2");
                            oren.Name += " Not Changed";
                            await session2.SaveChangesAsync();
                        }
                        await store2.AsyncDatabaseCommands.PutAttachmentAsync("fitzchak", null, new MemoryStream(new byte[] { 6 }), new RavenJObject());

						await SmugglerOperation.Between(new SmugglerBetweenOptions
						{
							From = new RavenConnectionStringOptions { Url = "http://localhost:8079", DefaultDatabase = "Database1" },
							To = new RavenConnectionStringOptions { Url = "http://localhost:8078", DefaultDatabase = "Database2" }
						}, new SmugglerOptions
						{
							Incremental = true,
						});

                        using (var session2 = store2.OpenAsyncSession("Database2"))
                        {
                            Assert.Equal(3, await session2.Query<User>().CountAsync());
                            Assert.Equal("Oren Eini Changed", (await session2.LoadAsync<User>("users/1")).Name);
                            Assert.Equal("Fitzchak Yitzchaki Not Changed", (await session2.LoadAsync<User>("users/2")).Name); // Test that this value won't be overwritten by the export server
                        }

                        Assert.Equal(3, (await store2.AsyncDatabaseCommands.GetAttachmentsAsync(Etag.Empty, 25)).Length);
                        await AssertAttachmentContent(store2, "ayende", new byte[] {4});
                        await AssertAttachmentContent(store2, "fitzchak", new byte[] {6}); // Test that this value won't be overwritten by the export server
                    }
                }
            }
        }

	    [Fact]
	    public async Task ShouldSupportIncrementalFromTwoServers()
	    {
		    using (var server1 = GetNewServer(port: 8079))
		    using (var store1 = NewRemoteDocumentStore(ravenDbServer: server1, databaseName: "Database1"))
		    {
			    using (var session = store1.OpenAsyncSession("Database1"))
			    {
				    await session.StoreAsync(new User {Name = "Oren Eini"});
				    await session.StoreAsync(new User {Name = "Fitzchak Yitzchaki"});
				    await session.SaveChangesAsync();
			    }
			    await store1.AsyncDatabaseCommands.PutAttachmentAsync("ayende", null, new MemoryStream(new byte[] {13}), new RavenJObject());
			    await store1.AsyncDatabaseCommands.PutAttachmentAsync("fitzchak", null, new MemoryStream(new byte[] {12}), new RavenJObject());

			    using (var server2 = GetNewServer(port: 8078))
			    using (var store2 = NewRemoteDocumentStore(ravenDbServer: server2, databaseName: "Database2"))
			    {
				    using (var session = store2.OpenAsyncSession("Database2"))
				    {
					    await session.StoreAsync(new User {Name = "Oren Eini Server 2"});
					    await session.SaveChangesAsync();
				    }
				    await store2.AsyncDatabaseCommands.PutAttachmentAsync("ayende", null, new MemoryStream(new byte[] {23}), new RavenJObject());

				    using (var server3 = GetNewServer(port: 8077))
				    {
						await SmugglerOperation.Between(new SmugglerBetweenOptions
						{
							From = new RavenConnectionStringOptions { Url = "http://localhost:8079", DefaultDatabase = "Database1" },
							To = new RavenConnectionStringOptions { Url = "http://localhost:8077", DefaultDatabase = "Database3" }
						}, new SmugglerOptions
						{
							Incremental = true,
						});

						await SmugglerOperation.Between(new SmugglerBetweenOptions
						{
							From = new RavenConnectionStringOptions { Url = "http://localhost:8078", DefaultDatabase = "Database2" },
							To = new RavenConnectionStringOptions { Url = "http://localhost:8077", DefaultDatabase = "Database3" }
						}, new SmugglerOptions
						{
							Incremental = true,
						});
					  
					    using (var store3 = NewRemoteDocumentStore(ravenDbServer: server3, databaseName: "Database3"))
					    {
						    using (var session3 = store3.OpenAsyncSession("Database3"))
						    {
							    Assert.Equal(2, await session3.Query<User>().CountAsync());
							    Assert.Equal("Oren Eini Server 2", (await session3.LoadAsync<User>("users/1")).Name);
							    Assert.Equal("Fitzchak Yitzchaki", (await session3.LoadAsync<User>("users/2")).Name); // Test that the value from Database1 is there
						    }

						    Assert.Equal(2, (await store3.AsyncDatabaseCommands.GetAttachmentsAsync(Etag.Empty, 25)).Length);
						    await AssertAttachmentContent(store3, "ayende", new byte[] {23});
						    await AssertAttachmentContent(store3, "fitzchak", new byte[] {12}); // Test that the value from Database1 is there
					    }
				    }
			    }
		    }
	    }

	    private async Task AssertAttachmentContent(IDocumentStore store2, string attachmentKey, byte[] expectedData)
        {
            var attachment = await store2.AsyncDatabaseCommands.GetAttachmentAsync(attachmentKey);
            var data = await attachment.Data().ReadDataAsync();
            Assert.Equal(expectedData, data);
        }

        protected override void ModifyStore(DocumentStore documentStore)
        {
            documentStore.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
        }


        private async Task AssertDatabaseHasIndex<TIndex>(IDocumentStore store) where TIndex : AbstractIndexCreationTask, new()
        {
            var indexes = await store.AsyncDatabaseCommands.GetIndexesAsync(0, 25);
            var indexName = new TIndex().IndexName;
            Assert.True(indexes.Any(definition => definition.Name == indexName), "Index " + indexName + " is missing");
        }

        private async Task AssertDatabaseHasTransformer<TTransformer>(IDocumentStore store) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformers = await store.AsyncDatabaseCommands.GetTransformersAsync(0, 25);
            var transformerName = new TTransformer().TransformerName;
            Assert.True(transformers.Any(definition => definition.Name == transformerName), "Transformer " + transformerName + " is missing");
        }

        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class UsersIndex : AbstractIndexCreationTask<User>
        {
            public UsersIndex()
            {
                Map = users => from user in users
                               select new {user.Name};
            }
        }

        public class UsersTransformer : AbstractTransformerCreationTask<User>
        {
            public UsersTransformer()
            {
                TransformResults = users => from user in users
                                            select new {user.Name};
            }
        }
    }
}