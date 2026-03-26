using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB9927 : RavenTestBase
    {
        public RavenDB9927(ITestOutputHelper output) : base(output)
        {
        }

        public class ComplexIndex : AbstractIndexCreationTask<object>
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "ComplexIndex",
                    Maps = {@"from doc in docs.Incidents
select new {
    Date = doc.OccuredOn,
    Count = 1
}
"},
                    Reduce = @"from m in results
group m by new {
    Date = m.Date.Date
} into g
select new {
    Date = g.Key.Date,
    Count = g.Sum(x => x.Count)
}"
                };
            }
        }
        
        [RavenFact(RavenTestCategory.Indexes)]
        public void CanDefineComplexExpressionInGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new ComplexIndex());
            }
        }
        
    }
}
