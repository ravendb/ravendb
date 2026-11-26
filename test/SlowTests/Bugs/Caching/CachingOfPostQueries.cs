using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Size = Sparrow.Size;

namespace SlowTests.Bugs.Caching
{
    public class CachingOfPostQueries : RavenTestBase
    {
        public CachingOfPostQueries(ITestOutputHelper output) : base(output)
        {
        }

        private class Person
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }

        private class PersonsIndex : AbstractIndexCreationTask<Person>
        {
            public PersonsIndex()
            {
                Map = results => from result in results
                                 select new Person
                                 {
                                     Name = result.Name
                                 };
            }
        }

        private static void InitData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Person
                {
                    Name = "Johnny",
                    Age = 26
                });
                session.SaveChanges();
            }
        }

        private IDocumentStore GetTestStore(Options options)
        {
            var store = GetDocumentStore(options);

            new PersonsIndex().Execute(store);
            InitData(store);
            Indexes.WaitForIndexing(store);
            return store;
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CachedGetQyuery(Options options)
        {
            using (var store = GetTestStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var response = session.Query<Person, PersonsIndex>().FirstOrDefault(x => x.Name == "Johnny");
                    Assert.Equal(session.Advanced.NumberOfRequests, 1);
                    Assert.Equal(1, session.Advanced.RequestExecutor.Cache.NumberOfItems);
                    response = session.Query<Person, PersonsIndex>().FirstOrDefault(x => x.Name == "Johnny");
                    Assert.Equal(session.Advanced.NumberOfRequests, 2);
                    Assert.Equal(1, session.Advanced.RequestExecutor.Cache.NumberOfItems);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CachedPostQyuery(Options options)
        {
            using (var store = GetTestStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var response = session.Query<Person, PersonsIndex>().FirstOrDefault(x => x.Name != "Jane" && x.Name != "Mika" && x.Name != "Michael" && x.Name != "Samuel");
                    Assert.Equal(session.Advanced.NumberOfRequests, 1);
                    Assert.Equal(1, session.Advanced.RequestExecutor.Cache.NumberOfItems);
                    response = session.Query<Person, PersonsIndex>().FirstOrDefault(x => x.Name != "Jane" && x.Name != "Mika" && x.Name != "Michael" && x.Name != "Samuel");
                    Assert.Equal(session.Advanced.NumberOfRequests, 2);
                    Assert.Equal(1, session.Advanced.RequestExecutor.Cache.NumberOfItems);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CachedFacetsGetRequest(Options options)
        {
            using (var store = GetTestStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var response = session.Query<Person, PersonsIndex>().Where(x => x.Name == "Johnny").AggregateBy(new[]
                        {
                            new Facet
                            {
                                FieldName = "Age"
                            }
                        })
                        .Execute();

                    Assert.Equal(1, session.Advanced.RequestExecutor.Cache.NumberOfItems);
                    response = session.Query<Person, PersonsIndex>().Where(x => x.Name == "Johnny").AggregateBy(new[]
                        {
                            new Facet
                            {
                                FieldName = "Age"
                            }
                        })
                        .Execute();

                    Assert.Equal(1, session.Advanced.RequestExecutor.Cache.NumberOfItems);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CachedFacetsPostRequest(Options options)
        {
            using (var store = GetTestStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var response = session.Query<Person, PersonsIndex>().Where(x => x.Name == "Johnny").AggregateBy(Enumerable.Range(1, 200).Select(x => new Facet()
                    {
                        FieldName = "Age" + x
                    }))
                        .Execute();
                    Assert.Equal(1, session.Advanced.RequestExecutor.Cache.NumberOfItems);
                    response = session.Query<Person, PersonsIndex>().Where(x => x.Name == "Johnny").AggregateBy(Enumerable.Range(1, 200).Select(x => new Facet()
                    {
                        FieldName = "Age" + x
                    }))
                        .Execute();
                    Assert.Equal(1, session.Advanced.RequestExecutor.Cache.NumberOfItems);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public async Task CachedMultiFacetsRequest(Options options)
        {
            using (var store = GetTestStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.Query<Person, PersonsIndex>()
                        .Where(x => x.Name == "Johnny")
                        .Skip(0)
                        .Take(16)
                        .AggregateBy(new List<Facet>
                        {
                            new Facet
                            {
                                FieldName = "Age"
                            }
                        })
                        .ExecuteAsync();

                    Assert.Equal(1, session.Advanced.RequestExecutor.Cache.NumberOfItems);

                    await session.Query<Person, PersonsIndex>()
                        .Where(x => x.Name == "Johnny")
                        .Skip(0)
                        .Take(16)
                        .AggregateBy(new List<Facet>
                        {
                            new Facet
                            {
                                FieldName = "Age"
                            }
                        })
                        .ExecuteAsync();

                    Assert.Equal(1, session.Advanced.RequestExecutor.Cache.NumberOfItems);
                }
            }
        }

        // RavenDB-28147: while the database does not change and the cached responses fit within MaxHttpCacheSize,
        // a repeated query is answered from the cache (304), however many cache entries were replaced before.
        [RavenFact(RavenTestCategory.ClientApi)]
        public void Unchanged_Queries_Should_Be_Served_From_Cache_After_One_Document_Was_Updated_And_Reloaded_Many_Times()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.MaxHttpCacheSize = new Size(1, SizeUnit.Megabytes)
            });

            using (var session = store.OpenSession())
            {
                session.Store(new Item { Payload = RandomString(40_000) }, "items/large");
                session.Store(new Item { Payload = "a" }, "items/a");
                session.Store(new Item { Payload = "b" }, "items/b");
                session.SaveChanges();
            }

            // Update one ~40 KB document and read it back 100 times. Each read replaces the same cache entry,
            // so the cache never holds more than one copy of it: far below the 1 MB limit.
            for (var i = 0; i < 100; i++)
            {
                using (var session = store.OpenSession())
                {
                    var item = session.Load<Item>("items/large");
                    item.Version = i + 1;
                    session.SaveChanges();
                }
            }

            // No writes from here on. After the first round, every query should be answered from the cache (304).
            const int rounds = 20;
            var servedFromCache = 0;
            for (var round = 0; round < rounds; round++)
            {
                foreach (var id in new[] { "items/a", "items/b" })
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Query<Item>()
                            .Statistics(out var stats)
                            .Where(x => x.Id == id)
                            .ToList();

                        Assert.Single(results);

                        if (stats.DurationInMs == -1)
                            servedFromCache++;
                    }
                }
            }

            // A low-memory cleanup may legitimately evict an entry while the test runs, so require most repeats to be
            // answered from the cache, not all of them.
            var repeatedQueries = 2 * (rounds - 1);
            Assert.True(servedFromCache * 2 > repeatedQueries,
                $"Only {servedFromCache} of {repeatedQueries} repeated queries were answered from the cache.");
        }

        private static string RandomString(int length)
        {
            var random = new Random(12345);
            return string.Create(length, random, (chars, r) =>
            {
                for (var i = 0; i < chars.Length; i++)
                    chars[i] = (char)('a' + r.Next(26));
            });
        }

        private class Item
        {
            public string Id { get; set; }
            public string Payload { get; set; }
            public int Version { get; set; }
        }
    }
}
