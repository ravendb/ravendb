using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

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

        [Theory]
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

        [Theory]
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

        [Theory]
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

        [Theory]
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

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
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
    }
}
