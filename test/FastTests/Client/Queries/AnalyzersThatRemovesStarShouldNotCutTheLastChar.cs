using System.Linq;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Queries
{
    public class AnalyzersThatRemovesStarShouldNotCutTheLastChar : RavenTestBase
    {
        public AnalyzersThatRemovesStarShouldNotCutTheLastChar(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanDoPrefixQueryOnAnalyzedFields()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                new FooBarIndex().Execute(store);
                session.Store(new Foo { Bar = "Shalom" });
                session.Store(new Foo { Bar = "Salam" });
                session.SaveChanges();
                var res = session.Query<Foo, FooBarIndex>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Single(x => x.Bar.StartsWith("Sh"));
                Assert.Equal(res.Bar, "Shalom");
            }
        }

        public class Foo
        {
            public string Bar { get; set; }
        }

        public class FooBarIndex : AbstractIndexCreationTask<Foo>
        {
            public FooBarIndex()
            {
                Map = foos => from foo in foos select new { foo.Bar };
                Analyzers.Add(c => c.Bar, typeof(Lucene.Net.Analysis.Standard.StandardAnalyzer).ToString());
            }
        }
    }
}
