using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Bugs.Vlko
{
    public class RelationIdIndex : RavenTestBase
    {
        public RelationIdIndex(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanBeUsedForTransformResultsWithDocumentId(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new ThorIndex().Execute(store);

                var relId = Guid.NewGuid();
                using (var s = store.OpenSession())
                {
                    s.Store(new Thor
                    {
                        Id = Guid.NewGuid().ToString(),
                        Name = "Thor",
                        Rel = new Relation
                        {
                            Id = relId
                        }
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var objects = s.Query<Thor, ThorIndex>()
                            .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(3)))
                            .ToArray();
                    Assert.Equal(1, objects.Length);
                    Assert.Equal(relId, objects[0].Rel.Id);

                    objects = s.Query<Thor, ThorIndex>()
                            .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(3)))
                            .Where(item => item.Rel.Id == relId)
                            .ToArray();

                    Assert.Equal(1, objects.Length);
                    Assert.Equal(relId, objects[0].Rel.Id);
                }
            }
        }

        private class ThorIndex : AbstractIndexCreationTask<Thor>
        {
            public ThorIndex()
            {
                Map = thors => from doc in thors
                               select new { doc.Name, Rel_Id = doc.Rel.Id };
            }
        }

        private class Thor
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public Relation Rel { get; set; }
        }

        private class Relation
        {
            public Guid Id { get; set; }
        }
    }
}
