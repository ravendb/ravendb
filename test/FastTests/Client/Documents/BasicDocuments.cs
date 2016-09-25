using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Extensions;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;

using Xunit;

namespace FastTests.Client.Documents
{
    public class BasicDocuments : RavenTestBase
    {
        [Fact]
        public async Task CanStoreAnonymousObject()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new { Name = "Fitzchak" });
                    await session.StoreAsync(new { Name = "Arek" });

                    await session.SaveChangesAsync();
                }
            }
        }

        [Fact]
        public async Task GetAsync()
        {
            using (var store = GetDocumentStore())
            {
                var dummy = RavenJObject.FromObject(new User());
                dummy.Remove("Id");

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" });
                    await session.StoreAsync(new User { Name = "Arek" });

                    await session.SaveChangesAsync();
                }

                var docs = await store.AsyncDatabaseCommands.GetAsync(new[] { "users/1", "users/2" }, null);
                Assert.Equal(2, docs.Results.Count);

                var doc1 = docs.Results[0];
                var doc2 = docs.Results[1];

                Assert.NotNull(doc1);
                Assert.True(doc1.ContainsKey("@metadata"));
                Assert.Equal(dummy.Keys.Count + 1, doc1.Keys.Count); // +1 for @metadata

                Assert.NotNull(doc2);
                Assert.True(doc2.ContainsKey("@metadata"));
                Assert.Equal(dummy.Keys.Count + 1, doc2.Keys.Count); // +1 for @metadata

                var user1 = docs.Results[0].JsonDeserialization<User>();
                var user2 = docs.Results[1].JsonDeserialization<User>();

                Assert.Equal("Fitzchak", user1.Name);
                Assert.Equal("Arek", user2.Name);

                docs = await store.AsyncDatabaseCommands.GetAsync(new[] { "users/1", "users/2" }, null, metadataOnly: true);

                doc1 = docs.Results[0];
                doc2 = docs.Results[1];

                Assert.NotNull(doc1);
                Assert.True(doc1.ContainsKey("@metadata"));
                Assert.Equal(1, doc1.Keys.Count);

                Assert.NotNull(doc2);
                Assert.True(doc2.ContainsKey("@metadata"));
                Assert.Equal(1, doc2.Keys.Count);
            }
        }

        [Fact]
        public async Task GetAsyncWithTransformer()
        {
            using (var store = GetDocumentStore())
            {
                var transformer = new Transformer();
                transformer.Execute(store);

                var dummy = RavenJObject.FromObject(new User());
                dummy.Remove("Id");

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" });
                    await session.StoreAsync(new User { Name = "Arek" });

                    await session.SaveChangesAsync();
                }

                var docs = await store.AsyncDatabaseCommands.GetAsync(new[] { "users/1", "users/2" }, null, transformer: transformer.TransformerName);
                Assert.Equal(2, docs.Results.Count);

                var doc1 = docs.Results[0];
                var doc2 = docs.Results[1];

                Assert.NotNull(doc1);
                Assert.True(doc1.ContainsKey("@metadata"));
                Assert.Equal(1 + 1, doc1.Keys.Count); // +1 for @metadata

                Assert.NotNull(doc2);
                Assert.True(doc2.ContainsKey("@metadata"));
                Assert.Equal(1 + 1, doc2.Keys.Count); // +1 for @metadata

                var values1 = docs.Results[0].Value<RavenJArray>("$values");
                var values2 = docs.Results[1].Value<RavenJArray>("$values");

                Assert.Equal(1, values1.Length);
                Assert.Equal(1, values2.Length);

                var user1 = values1[0].JsonDeserialization<Transformer.Result>();
                var user2 = values2[0].JsonDeserialization<Transformer.Result>();

                Assert.Equal("Fitzchak", user1.Name);
                Assert.Equal("Arek", user2.Name);

                docs = await store.AsyncDatabaseCommands.GetAsync(new[] { "users/1", "users/2" }, null, transformer: transformer.TransformerName, metadataOnly: true);

                doc1 = docs.Results[0];
                doc2 = docs.Results[1];

                Assert.NotNull(doc1);
                Assert.True(doc1.ContainsKey("@metadata"));
                Assert.Equal(1, doc1.Keys.Count);

                Assert.NotNull(doc2);
                Assert.True(doc2.ContainsKey("@metadata"));
                Assert.Equal(1, doc2.Keys.Count);
            }
        }

        private class Transformer : AbstractTransformerCreationTask<User>
        {
            public class Result
            {
                public string Name { get; set; }
            }

            public Transformer()
            {
                TransformResults = results => from result in results
                                              select new
                                              {
                                                  result.Name
                                              };
            }
        }
    }
}