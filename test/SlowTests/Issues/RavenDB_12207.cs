using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12207 : RavenTestBase
    {
        private class BaseDocument
        {
            public string Id { get; set; }
            public int Int { get; set; }
        }

        private class SubDocument1 : BaseDocument
        {
            public int Prop1 { get; set; }
        }

        private class SubSubDocument1 : SubDocument1
        {
            public string SomeOtherProp { get; set; }
        }

        private class SubDocument2 : BaseDocument
        {
            public int Prop2 { get; set; }
        }

        private class SubClassIndex : AbstractIndexCreationTask<SubDocument1, SubClassIndex.ReduceResult>
        {
            public class ReduceResult
            {
                public string Id { get; set; }
                public int Int { get; set; }
            }

            public SubClassIndex()
            {
                Map = foo => foo.WhereEntityIs<BaseDocument>("SubDocument1s", "SubSubDocument1s")
                    .Select(d => new ReduceResult // NOTE: we also tried "foo.WhereEntityIs<SubDocument1>()" but then no documents are found
                    {
                        Id = d.Id,
                        Int = d.Int
                    });
            }
        }

        private class SubClassIndex2 : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = nameof(SubClassIndex2),
                    Maps =
                    {
                        @"
from d in docs.WhereEntityIs(""SubDocument1s"", ""SubSubDocument1s"")
select new 
{
    Id = Id(d),
    Int = d.Int
}
"
                    }
                };
            }
        }

        public void Setup(DocumentStore store)
        {
            new SubClassIndex().Execute(store);
            new SubClassIndex2().Execute(store);

            using (var s = store.OpenSession())
            {
                s.Store(new SubDocument1 { Id = "One", Prop1 = 111, Int = 99 });
                s.Store(new SubDocument2 { Id = "Two", Prop2 = 222, Int = 99 });
                s.Store(new SubSubDocument1 { Id = "Three", Prop1 = 333, Int = 99 });

                s.SaveChanges();
            }

            WaitForIndexing(store);
        }

        [Fact]
        public void CanQuerySubIndexReduceResult()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    var results = s.Query<SubClassIndex.ReduceResult, SubClassIndex>().Where(r => r.Int == 99).ToList();

                    Assert.Equal(2, results.Count); // FAILS - should only find SubDocument1 and SubSubDocument1, but all three are actually found
                }

                using (var s = store.OpenSession())
                {
                    var results = s.Query<SubClassIndex.ReduceResult, SubClassIndex2>().Where(r => r.Int == 99).ToList();

                    Assert.Equal(2, results.Count); // FAILS - should only find SubDocument1 and SubSubDocument1, but all three are actually found
                }
            }
        }
    }
}
