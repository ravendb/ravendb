// -----------------------------------------------------------------------
//  <copyright file="SmugglerBetweenTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Remote;
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

                using (var server2 = GetNewServer(port: 8078))
                {
					using (var store2 = NewRemoteDocumentStore(ravenDbServer: server2, databaseName: "Database2"))
					{
					    var smuggler = new DatabaseSmuggler(
                            new DatabaseSmugglerOptions(), 
                            new DatabaseSmugglerRemoteSource(
                                new DatabaseSmugglerRemoteConnectionOptions
                                {
                                    Url = "http://localhost:8079",
                                    Database = "Database1"
                                }), 
                            new DatabaseSmugglerRemoteDestination(
                                new DatabaseSmugglerRemoteConnectionOptions
                                {
                                    Url = "http://localhost:8078",
                                    Database = "Database2"
                                }));

					    await smuggler.ExecuteAsync();
                    
                        await AssertDatabaseHasIndex<UsersIndex>(store2);
                        await AssertDatabaseHasTransformer<UsersTransformer>(store2);

                        using (var session2 = store2.OpenAsyncSession("Database2"))
                        {
                            Assert.Equal(2, await session2.Query<User>().CountAsync());
                        }                     
                    }
                }
            }
        }

        [Fact]
        public async Task ShouldSupportIncremental()
        {
            throw new NotImplementedException();

            using (var server1 = GetNewServer(port: 8079))
            using (var store1 = NewRemoteDocumentStore(ravenDbServer: server1, databaseName: "Database1"))
            {
                using (var session = store1.OpenAsyncSession("Database1"))
                {
                    await session.StoreAsync(new User {Name = "Oren Eini"});
                    await session.StoreAsync(new User {Name = "Fitzchak Yitzchaki"});
                    await session.SaveChangesAsync();
                }

                using (var server2 = GetNewServer(port: 8078))
                {
					using (var store2 = NewRemoteDocumentStore(ravenDbServer: server2, databaseName: "Database2"))
					{
						//var smugglerApi = new SmugglerDatabaseApi(new SmugglerDatabaseOptions
						//{
						//	Incremental = true,
						//});

                        //await smugglerApi.Between(new SmugglerBetweenOptions<RavenConnectionStringOptions>
						//{
						//	From = new RavenConnectionStringOptions { Url = "http://localhost:8079", DefaultDatabase = "Database1" },
						//	To = new RavenConnectionStringOptions { Url = "http://localhost:8078", DefaultDatabase = "Database2" }
						//});

						using (var session = store1.OpenAsyncSession("Database1"))
						{
							var oren = await session.LoadAsync<User>("users/1");
							oren.Name += " Changed";
							await session.StoreAsync(new User {Name = "Daniel Dar"});
							await session.SaveChangesAsync();
						}
                    
                        using (var session2 = store2.OpenAsyncSession("Database2"))
                        {
                            var oren = await session2.LoadAsync<User>("users/2");
                            oren.Name += " Not Changed";
                            await session2.SaveChangesAsync();
                        }

                        //await smugglerApi.Between(new SmugglerBetweenOptions<RavenConnectionStringOptions>
						//{
						//	From = new RavenConnectionStringOptions { Url = "http://localhost:8079", DefaultDatabase = "Database1" },
						//	To = new RavenConnectionStringOptions { Url = "http://localhost:8078", DefaultDatabase = "Database2" }
						//});

					    WaitForIndexing(store2);
                        using (var session2 = store2.OpenAsyncSession("Database2"))
                        {
                            Assert.Equal(3, await session2.Query<User>().CountAsync());
                            Assert.Equal("Oren Eini Changed", (await session2.LoadAsync<User>("users/1")).Name);
                            Assert.Equal("Fitzchak Yitzchaki Not Changed", (await session2.LoadAsync<User>("users/2")).Name); // Test that this value won't be overwritten by the export server
                        }
                    }
                }
            }
        }

	    [Fact]
	    public async Task ShouldSupportIncrementalFromTwoServers()
	    {
            throw new NotImplementedException();

            using (var server1 = GetNewServer(port: 8079))
		    using (var store1 = NewRemoteDocumentStore(ravenDbServer: server1, databaseName: "Database1"))
		    {
			    using (var session = store1.OpenAsyncSession("Database1"))
			    {
				    await session.StoreAsync(new User {Name = "Oren Eini"});
				    await session.StoreAsync(new User {Name = "Fitzchak Yitzchaki"});
				    await session.SaveChangesAsync();
			    }

			    using (var server2 = GetNewServer(port: 8078))
			    using (var store2 = NewRemoteDocumentStore(ravenDbServer: server2, databaseName: "Database2"))
			    {
				    using (var session = store2.OpenAsyncSession("Database2"))
				    {
					    await session.StoreAsync(new User {Name = "Oren Eini Server 2"});
					    await session.SaveChangesAsync();
				    }

				    using (var server3 = GetNewServer(port: 8077))
				    {
						using (var store3 = NewRemoteDocumentStore(ravenDbServer: server3, databaseName: "Database3"))
						{
							//var smugglerApi = new SmugglerDatabaseApi(new SmugglerDatabaseOptions
							//{
							//	Incremental = true,
							//});

                            //await smugglerApi.Between(new SmugglerBetweenOptions<RavenConnectionStringOptions>
							//{
							//	From = new RavenConnectionStringOptions { Url = "http://localhost:8079", DefaultDatabase = "Database1" },
							//	To = new RavenConnectionStringOptions { Url = "http://localhost:8077", DefaultDatabase = "Database3" }
							//});

                            //await smugglerApi.Between(new SmugglerBetweenOptions<RavenConnectionStringOptions>
							//{
							//	From = new RavenConnectionStringOptions { Url = "http://localhost:8078", DefaultDatabase = "Database2" },
							//	To = new RavenConnectionStringOptions { Url = "http://localhost:8077", DefaultDatabase = "Database3" }
							//});  
					    
						    using (var session3 = store3.OpenAsyncSession("Database3"))
						    {
							    Assert.Equal(2, await session3.Query<User>().CountAsync());
							    Assert.Equal("Oren Eini Server 2", (await session3.LoadAsync<User>("users/1")).Name);
							    Assert.Equal("Fitzchak Yitzchaki", (await session3.LoadAsync<User>("users/2")).Name); // Test that the value from Database1 is there
						    }
					    }
				    }
			    }
		    }
	    }

        protected override void ModifyStore(DocumentStore documentStore)
        {
            documentStore.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
        }


        internal static async Task AssertDatabaseHasIndex<TIndex>(IDocumentStore store) where TIndex : AbstractIndexCreationTask, new()
        {
            var indexes = await store.AsyncDatabaseCommands.GetIndexesAsync(0, 25);
            var indexName = new TIndex().IndexName;
            Assert.True(indexes.Any(definition => definition.Name == indexName), "Index " + indexName + " is missing");
        }

        internal static async Task AssertDatabaseHasTransformer<TTransformer>(IDocumentStore store) where TTransformer : AbstractTransformerCreationTask, new()
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