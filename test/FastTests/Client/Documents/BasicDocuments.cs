using System;
using System.Threading.Tasks;

using Raven.Abstractions.Extensions;
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

        [Fact(Skip = "Implement transformers first.")]
        public Task GetAsyncWithTransformer()
        {
            throw new NotImplementedException();
        }
    }
}