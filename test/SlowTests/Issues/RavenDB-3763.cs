using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3736
    {
        public class IsInTriggersSyncFromAsync : RavenTestBase
        {
            private readonly DocumentStore _store;

            public IsInTriggersSyncFromAsync()
            {
                _store = GetDocumentStore();

                new Index().Execute(_store);
                using (var session = _store.OpenSession())
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

            [Fact]
            public async Task IsInTriggersSyncFromAsyncException()
            {
                using (var session = _store.OpenAsyncSession())
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
            [Fact]
            public async Task IsInTriggersSyncFromAsyncWorks()
            {
                using (var session = _store.OpenAsyncSession())
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
            [Fact]
            public async Task WithoutIsInItWorks()
            {
                using (var session = _store.OpenAsyncSession())
                {
                    var entity = await session.Query<Entity, Index>()
                        .Customize(q => q.WaitForNonStaleResults())
                        .Where(e => e.Id == "solo")
                        .FirstOrDefaultAsync();
                    Assert.NotNull(entity);
                    Assert.Equal("solo", entity.Id);
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
