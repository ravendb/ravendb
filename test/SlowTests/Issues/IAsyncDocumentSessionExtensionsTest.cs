using System;
using System.Globalization;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Tests.Infrastructure;
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanStreamStartWithAsync(Options options)
        {
            using (var store = GetDocumentStore(options))
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
                var id = "";
                string tmp;
                using (var session = store.OpenAsyncSession())
                {
                    await using (var reader = await session.Advanced.StreamAsync<User>(startsWith: "users/"))
                    {
                        while (await reader.MoveNextAsync())
                        {
                            count++;
                            tmp = reader.Current.Id;
                            Assert.True(String.Compare(id,tmp, StringComparison.OrdinalIgnoreCase) < 0);
                            id = tmp;
                            Assert.IsType<User>(reader.Current.Document);
                        }
                    }
                }
                Assert.Equal(10, count);
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanStreamWithSkipAsync(Options options)
        {
            using (var store = GetDocumentStore(options))
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
                    await using (var reader = await session.Advanced.StreamAsync<User>(startsWith: "users/", start: 6, pageSize: 10))
                    {
                        while (await reader.MoveNextAsync())
                        {
                            count++;
                            Assert.IsType<User>(reader.Current.Document);
                        }
                    }
                }
                Assert.Equal(4, count);
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanStreamByLastModifiedOrderAsync(Options options)
        {
            using (var store = GetDocumentStore(options))
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
                var lastModified = DateTime.MaxValue;
                DateTime tmp;
                using (var session = store.OpenAsyncSession())
                {
                    await using (var reader = await session.Advanced.StreamAsync<User>(""))
                    {
                        while (await reader.MoveNextAsync())
                        {
                            count++;
                            tmp = DateTime.ParseExact(reader.Current.Metadata[Constants.Documents.Metadata.LastModified].ToString(), "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'",
                                CultureInfo.InvariantCulture, DateTimeStyles.None);
                            Assert.True(lastModified >= tmp);
                            lastModified = tmp;
                            Assert.IsType<User>(reader.Current.Document);
                        }
                    }
                }
                Assert.Equal(11, count);
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanStreamStartWithAndStartAfter(Options options)
        {
            using (var store = GetDocumentStore(options))
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
