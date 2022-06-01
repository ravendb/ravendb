using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3736
    {
        public class IsInTriggersSyncFromAsync : RavenTestBase
        {
        public IsInTriggersSyncFromAsync(ITestOutputHelper output) : base(output)
        {
        }

            private void CreateData(IDocumentStore store)
            {
                new Index().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Entity
                    {
                        Id = "solo",
                        Tokens = new List<string>
                        {
                            "ABC",
                            "DEF"
                        }
                    });
                    session.SaveChanges();
                }
            }

            [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
            [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
            public async Task IsInTriggersSyncFromAsyncException(Options options)
            {
                using (var store = GetDocumentStore(options))
                {
                    CreateData(store);

                    using (var session = store.OpenAsyncSession())
                    {
                        var queryToken = "ABC";
                        try
                        {
                            var entity = await session.Query<Entity, Index>()
                                .Customize(q => q.WaitForNonStaleResults())
                                .Where(e => e.Id == "solo" && queryToken.In((e.Tokens)))
                                .FirstOrDefaultAsync();
                        }
                        catch (Exception e)
                        {
                            Assert.NotNull(e);
                        }
                    }
                }
            }

            [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
            [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
            public async Task IsInTriggersSyncFromAsyncWorks(Options options)
            {
                using (var store = GetDocumentStore(options))
                {
                    CreateData(store);

                    using (var session = store.OpenAsyncSession())
                    {
                        var queryToken = "ABC";
                        var entity = await session.Query<Entity, Index>()
                            .Customize(q => q.WaitForNonStaleResults())
                            .Where(e => e.Id == "solo" && e.Tokens.Contains(queryToken))
                            .FirstOrDefaultAsync();
                        Assert.NotNull(entity);
                        Assert.Equal("solo", entity.Id);
                    }
                }
            }

            [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
            [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
            public async Task WithoutIsInItWorks(Options options)
            {
                using (var store = GetDocumentStore(options))
                {
                    CreateData(store);

                    using (var session = store.OpenAsyncSession())
                    {
                        var entity = await session.Query<Entity, Index>()
                            .Customize(q => q.WaitForNonStaleResults())
                            .Where(e => e.Id == "solo")
                            .FirstOrDefaultAsync();
                        Assert.NotNull(entity);
                        Assert.Equal("solo", entity.Id);
                    }
                }
            }

            private class Entity
            {
                public string Id { get; set; }
                public List<string> Tokens { get; set; }
            }

            private class Index : AbstractIndexCreationTask<Entity>
            {
                public Index()
                {
                    Map = entities => from entity in entities
                                      select new
                                      {
                                          entity.Tokens
                                      };
                }
            }

        }
    }
}
