// -----------------------------------------------------------------------
//  <copyright file="SmugglerBetweenTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Smuggler;
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
                await new UsersIndex().ExecuteAsync(store1);
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
                    await SmugglerOp.Between(new SmugglerBetweenOptions
                    {
                        From = new RavenConnectionStringOptions
                        {
                            Url = "http://localhost:8079",
                            DefaultDatabase = "Database1",
                        },
                        To = new RavenConnectionStringOptions
                        {
                            Url = "http://localhost:8078",
                            DefaultDatabase = "Database2",
                        },
                    });

                    using (var store2 = NewRemoteDocumentStore(ravenDbServer: server2, databaseName: "Database2"))
                    {
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