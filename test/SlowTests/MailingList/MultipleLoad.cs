using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class MultipleLoad : RavenTestBase
    {
        public MultipleLoad(ITestOutputHelper output) : base(output)
        {
        }

        private class TestableDTO
        {
            public string Id
            {
                get;
                set;
            }

            public IDictionary<string, IList<string>> data
            {
                get;
                set;
            }
        }

        public class TestableSubDTO
        {
            public string Id { get; set; }
            public IList<string> data { get; set; }
        }
        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanSelectValuesWithOnlySingleIdentity(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new TestableSubDTO
                    {
                        data = new List<string> { "a1", "a2", "a3" }
                    });
                    await session.StoreAsync(new TestableSubDTO
                    {
                        data = new List<string> { "b1", "b2", "b3" }
                    });
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var stored = await session.Query<TestableSubDTO>().Take(2).ToListAsync();

                    var testable = new TestableDTO
                    {
                        data = new Dictionary<string, IList<string>> {
                            { "subdata", stored.Select(c => c.Id).ToList() }
                        }
                    };

                    await session.StoreAsync(testable);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenSession())
                {
                    var results = from item in session.Query<TestableDTO>()
                                  let subitem = session.Load<TestableSubDTO>("TestableSubDTOs/1-A")
                                  select new
                                  {
                                      Id = item.Id,
                                      data = item.data,
                                      values = subitem
                                  };

                    var first = results.First();

                    Assert.Collection(first.values.data.Select(n => n),
                        i => Assert.Equal("a1", i),
                        i => Assert.Equal("a2", i),
                        i => Assert.Equal("a3", i));
                }
            }
        }
        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanSelectValuesWithCollection(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new TestableSubDTO
                    {
                        data = new List<string> { "a1", "a2", "a3" }
                    });
                    await session.StoreAsync(new TestableSubDTO
                    {
                        data = new List<string> { "b1", "b2", "b3" }
                    });
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var stored = await session.Query<TestableSubDTO>().Take(2).ToListAsync();

                    var testable = new TestableDTO
                    {
                        data = new Dictionary<string, IList<string>> {
                            { "subdata", stored.Select(c => c.Id).ToList() }
                        }
                    };

                    await session.StoreAsync(testable);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenSession())
                {
                    var results = from item in session.Query<TestableDTO>()
                                  let subitems = RavenQuery.Load<TestableSubDTO>(item.data["subdata"].Select(c => c))
                                  select new
                                  {
                                      Id = item.Id,
                                      data = item.data,
                                      values = subitems.SelectMany(x=>x.data)
                                  };

                    var first = results.First();
                    // turn the raw data into what we know we expect

                    // the flattened collection should equal a simple array
                    // of all of the sub-values in sequence
                    Assert.Collection(first.values,
                        i => Assert.Equal("a1", i),
                        i => Assert.Equal("a2", i),
                        i => Assert.Equal("a3", i),
                        i => Assert.Equal("b1", i),
                        i => Assert.Equal("b2", i),
                        i => Assert.Equal("b3", i));
                }
            }
        }

        [Fact]
        public async Task CanSelectValuesWithCollection_Error()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new TestableSubDTO
                    {
                        data = new List<string> { "a1", "a2", "a3" }
                    });
                    await session.StoreAsync(new TestableSubDTO
                    {
                        data = new List<string> { "b1", "b2", "b3" }
                    });
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var stored = await session.Query<TestableSubDTO>().Take(2).ToListAsync();

                    var testable = new TestableDTO
                    {
                        data = new Dictionary<string, IList<string>> {
                            { "subdata", stored.Select(c => c.Id).ToList() }
                        }
                    };

                    await session.StoreAsync(testable);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenSession())
                {
                    var results = from item in session.Query<TestableDTO>()
                                  let subitems = session.Load<TestableSubDTO>(item.data["subdata"].Select(c => c))
                                  select new
                                  {
                                      Id = item.Id,
                                      data = item.data,
                                      values = subitems
                                  };

                    var ex = Assert.Throws<NotSupportedException>(() =>
                    {
                        GC.KeepAlive(results.First());
                    });
                    Assert.Equal("Using IDocumentSession.Load(IEnumerable<string> ids) inside a query is not supported. You should use RavenQuery.Load(IEnumerable<string> ids) instead", ex.Message);
                }
            }
        }
    }
}
