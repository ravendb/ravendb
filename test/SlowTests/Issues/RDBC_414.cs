using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDBC_414 : RavenTestBase
    {
        public RDBC_414(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Index_With_OrderBy_ThenBy_Should_Compile()
        {
            using (var store = GetDocumentStore())
            {
                new TestIndexWithThenBy().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc
                    {
                        Prop1 = "Prop1",
                        Prop2 = 2,
                        Prop3 = 3,
                        Prop4 = "Prop4"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Query<TestDoc, TestIndexWithThenBy>()
                        .ToList();

                    Assert.Equal(1, results.Count);
                }
            }
        }

        private class TestIndexWithThenBy : AbstractIndexCreationTask<TestDoc, Result>
        {
            public TestIndexWithThenBy()
            {
                Map = docs => docs.Select(doc => new Result { Prop1 = doc.Prop1, Prop2 = doc.Prop2, Prop3 = doc.Prop3 });
                Reduce = results => results
                    .GroupBy(result => new { result.Prop1 })
                    .Select(result => result.OrderBy(x => x.Prop2).ThenBy(x => x.Prop3).First())
                    .Select(result => new Result { Prop1 = result.Prop1, Prop2 = result.Prop2, Prop3 = result.Prop3 });
            }
        }

        private class TestDoc
        {
            public string Prop1 { get; set; }

            public int Prop2 { get; set; }

            public int Prop3 { get; set; }

            public string Prop4 { get; set; }
        }

        private class Result
        {
            public string Prop1 { get; set; }

            public int Prop2 { get; set; }

            public int Prop3 { get; set; }
        }
    }
}
