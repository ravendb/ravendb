using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Server.Documents.Sharding.Streaming;
using Sparrow.Json;
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
        public async Task CanStreamStartWithAsync2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 100; i++)
                    {
                        session.Store(new User
                        {
                            Name = "Geralt of Rivia " + i,
                            Description = "If I'm to choose between one evil and another, I'd rather not choose at all."
                        });
                    }
                    session.SaveChanges();
                }


                using (store.GetRequestExecutor().ContextPool.AllocateOperationContext(out var context))
                {
                    var cmd = new GetDocumentsCommand(store.Conventions, startWith: "users/", startAfter: null, matches: null, exclude: null, start: 0, pageSize: int.MaxValue,
                        metadataOnly: false);
                    await store.GetRequestExecutor().ExecuteAsync(cmd, context);

                    var results = cmd.Result.Results;
                    Assert.Equal(100, results.Length);

                    var docs = ConvertBlittableJsonReaderArrayToListOfDocuments(results);
                    AssertIsSortedByDocumentId(docs);
                }
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
        public async Task CanStreamWithSkipAsync2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 100; i++)
                    {
                        session.Store(new User
                        {
                            Name = "Geralt of Rivia " + i,
                            Description = "If I'm to choose between one evil and another, I'd rather not choose at all."
                        });
                    }
                    session.SaveChanges();
                }


                using (store.GetRequestExecutor().ContextPool.AllocateOperationContext(out var context))
                {
                    var cmd = new GetDocumentsCommand(store.Conventions, startWith: "users/", startAfter: null, matches: null, exclude: null, start: 6, pageSize: int.MaxValue,
                        metadataOnly: false);
                    await store.GetRequestExecutor().ExecuteAsync(cmd, context);

                    var results = cmd.Result.Results;
                    Assert.Equal(94, results.Length);

                    var docs = ConvertBlittableJsonReaderArrayToListOfDocuments(results);
                    AssertIsSortedByDocumentId(docs);
                }
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanStreamByLastModifiedOrderAsync2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 100; i++)
                    {
                        session.Store(new User
                        {
                            Name = "Geralt of Rivia " + i,
                            Description = "If I'm to choose between one evil and another, I'd rather not choose at all."
                        });
                    }
                    session.SaveChanges();
                }

                using (store.GetRequestExecutor().ContextPool.AllocateOperationContext(out var context))
                {
                    var cmd = new GetDocumentsCommand(start: 0, pageSize: int.MaxValue);
                    await store.GetRequestExecutor().ExecuteAsync(cmd, context);

                    var results = cmd.Result.Results;
                    Assert.Equal(101, results.Length);

                    var docs = ConvertBlittableJsonReaderArrayToListOfDocuments(results);
                    AssertIsSortedByLastModified(docs);
                }
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanStreamStartWithAndStartAfter2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 100; i++)
                    {
                        session.Store(new User
                        {
                            Name = "Geralt of Rivia " + i,
                            Description = "If I'm to choose between one evil and another, I'd rather not choose at all."
                        }, $"users/{i}");
                    }
                    session.SaveChanges();
                }

                using (store.GetRequestExecutor().ContextPool.AllocateOperationContext(out var context))
                {
                    var cmd = new GetDocumentsCommand(store.Conventions, startWith: "users/", start: 0, pageSize: int.MaxValue, startAfter: "users/95", matches: null, exclude: null, metadataOnly: false);
                    await store.GetRequestExecutor().ExecuteAsync(cmd, context);

                    var results = cmd.Result.Results;
                    Assert.Equal(4, results.Length);
                }
            }
        }

        private List<Raven.Server.Documents.Document> ConvertBlittableJsonReaderArrayToListOfDocuments(BlittableJsonReaderArray bjra)
        {
            var docs = new List<Raven.Server.Documents.Document>();
            foreach (var obj in bjra)
            {
                if (obj is BlittableJsonReaderObject blittable)
                {
                    var doc = ShardResultConverter.BlittableToDocumentConverter(blittable);
                    docs.Add(doc);
                }
            }

            return docs;
        }

        private void AssertIsSortedByDocumentId(List<Raven.Server.Documents.Document> docs)
        {
            var id = "";
            foreach (var doc in docs)
            {
                var tmp = doc.LowerId;
                Assert.True(string.Compare(id, tmp, StringComparison.OrdinalIgnoreCase) < 0);
                id = tmp;
            }
        }

        private void AssertIsSortedByLastModified(List<Raven.Server.Documents.Document> docs)
        {
            var lastModified = DateTime.MaxValue;
            foreach (var doc in docs)
            {
                if (doc.TryGetMetadata(out var metadata) == false)
                    throw new InvalidOperationException($"Couldn't get metadata for '{doc.LowerId}' document");

                var tmp = DateTime.ParseExact(metadata[Constants.Documents.Metadata.LastModified].ToString(), "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'",
                    CultureInfo.InvariantCulture, DateTimeStyles.None);
                Assert.True(lastModified >= tmp);
                lastModified = tmp;
            }
        }
    }
}
