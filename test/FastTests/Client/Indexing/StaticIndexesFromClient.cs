using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client.Indexing
{
    public class StaticIndexesFromClient : RavenTestBase
    {
        [Fact]
        public async Task Can_Put()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" });
                    await session.StoreAsync(new User { Name = "Arek" });

                    await session.SaveChangesAsync();
                }

                var input = new IndexDefinition
                {
                    Name = "Users_ByName",
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Type = IndexType.Map
                };

                await store
                    .Admin
                    .SendAsync(new PutIndexesOperation(new []{input}));

                var output = await store
                    .Admin
                    .SendAsync(new GetIndexOperation("Users_ByName"));

                Assert.Equal(1, output.Etag);
                Assert.True(input.Equals(output, compareIndexIds: false, ignoreFormatting: false));
            }
        }

        public class UserAndAge
        {
            public string Name { set; get; }
            public int Age { set; get; }
        }

        [Fact]
        public async Task Can_Put_And_Replace()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new UserAndAge { Name = "Boki", Age = 14});
                    await session.StoreAsync(new UserAndAge { Name = "Toli", Age = 5 });

                    await session.SaveChangesAsync();
                }

                var input = new IndexDefinition
                {
                    Maps = { "from user in docs.UserAndAges select new { user.Name }" },
                    Type = IndexType.Map,
                    Name = "Users_ByName"
                };

                var input2 = new IndexDefinition
                {
                    Maps = { "from user in docs.UserAndAges select new { user.Age }" },
                    Type = IndexType.Map,
                    Name = "Users_ByName"
                };

                await store
                    .Admin
                    .SendAsync(new PutIndexesOperation(new[] { input }));

                var output = await store
                    .Admin
                    .SendAsync(new GetIndexOperation("Users_ByName"));

                Assert.Equal(1, output.Etag);
                Assert.True(input.Equals(output, compareIndexIds: false, ignoreFormatting: false));

                await store
                    .Admin
                    .SendAsync(new PutIndexesOperation(new[] { input2 }));

                WaitForIndexing(store);

                output = await store
                    .Admin
                    .SendAsync(new GetIndexOperation("Users_ByName"));

                Assert.Equal(2, output.Etag);
                Assert.Equal("Users_ByName", output.Name);
                Assert.True(input2.Equals(output, compareIndexIds: false, ignoreFormatting: false));

            }
        }

        [Fact]
        public async Task Can_Put_Replace_And_Back_To_Original()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new UserAndAge { Name = "Boki", Age = 14 });
                    await session.StoreAsync(new UserAndAge { Name = "Toli", Age = 5 });

                    await session.SaveChangesAsync();
                }

                var input = new IndexDefinition
                {
                    Maps = { "from user in docs.UserAndAges select new { user.Name }" },
                    Type = IndexType.Map,
                    Name = "Users_ByName"
                };

                var input2 = new IndexDefinition
                {
                    Maps = { "from user in docs.UserAndAges select new { user.Age }" },
                    Type = IndexType.Map,
                    Name = "Users_ByName"
                };

                await store
                    .Admin
                    .SendAsync(new PutIndexesOperation(new[] { input }));

                var output = await store
                    .Admin
                    .SendAsync(new GetIndexOperation("Users_ByName"));

                Assert.Equal(1, output.Etag);
                Assert.True(input.Equals(output, compareIndexIds: false, ignoreFormatting: false));

                await store
                    .Admin
                    .SendAsync(new PutIndexesOperation(new[] { input2 }));

                await store
                   .Admin
                   .SendAsync(new PutIndexesOperation(new[] { input }));

                WaitForIndexing(store);

                output = await store
                    .Admin
                    .SendAsync(new GetIndexOperation("Users_ByName"));

                Assert.NotEqual(2, output.Etag);
                Assert.True(input.Equals(output, compareIndexIds: false, ignoreFormatting: false));

            }
        }
    }
}