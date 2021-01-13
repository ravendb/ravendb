using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class IAsyncDocumentSessionExtensionsTest : RavenTestBase
    {
        public IAsyncDocumentSessionExtensionsTest(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
        }

        [Fact]
        public async Task CanStreamStartWithAsync()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new User
                        {
                            Name = "Geralt of Rivia " + i,
                            Description = "If I'm to choose between one evil and another, I'd rather not choose at all."
                        });
                    }
                    session.SaveChanges();
                }

                int count = 0;
                using (var session = store.OpenAsyncSession())
                {
                    await using (var reader = await session.Advanced.StreamAsync<User>(startsWith: "users/"))
                    {
                        while (await reader.MoveNextAsync())
                        {
                            count++;
                            Assert.IsType<User>(reader.Current.Document);
                        }
                    }
                }
                Assert.Equal(10, count);
            }
        }

        [Fact]
        public void CanStreamStartWith()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new User
                        {
                            Name = "Geralt of Rivia " + i,
                            Description = "If I'm to choose between one evil and another, I'd rather not choose at all."
                        });
                    }
                    session.SaveChanges();
                }

                int count = 0;
                using (var session = store.OpenSession())
                {
                    using (var reader = session.Advanced.Stream<User>(startsWith: "users/"))
                    {
                        while (reader.MoveNext())
                        {
                            count++;
                            Assert.IsType<User>(reader.Current.Document);
                        }
                    }
                }
                Assert.Equal(10, count);
            }
        }

        [Fact]
        public async Task CanStreamStartWithAndStartAfter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.StoreAsync(new User(), "users/3");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var count = await GetStreamCount("users/0");
                    Assert.Equal(2, count);

                    count = await GetStreamCount("users/1");
                    Assert.Equal(1, count);

                    count = await GetStreamCount("users/2");
                    Assert.Equal(1, count);

                    count = await GetStreamCount("users/3");
                    Assert.Equal(0, count);

                    async Task<int> GetStreamCount(string startAfter)
                    {
                        var stream = await session.Advanced.StreamAsync<User>("users/", startAfter: startAfter);

                        var usersCount = 0;
                        while (await stream.MoveNextAsync())
                        {
                            usersCount++;
                        }

                        return usersCount;
                    }
                }
            }
        }
    }
}
