using System.Linq;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs
{
    public class RavenDB_19442 : RavenTestBase
    {
        public RavenDB_19442(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void WhenIndexingFailedOnItemTheRestIsAlsoCorrupted(Options options)
        {
            using var store = GetDocumentStore(options);
            {
                using var session = store.OpenSession();
                session.Store(new TestData()
                {
                    Identifier = "hehe",
                    Name = "a",
                    Second = "b"
                });
                session.Store(new TestData()
                {
                    Identifier = "hehe2",
                    Name = "a2",
                    Second = "b2"
                });

                session.Store(new TestData()
                {
                    Identifier = "hehe3",
                    Name = "a2",
                    Second = "b3"
                });

                session.SaveChanges();
            }

            new ComplexIndex().Execute(store);
            WaitForUserToContinueTheTest(store);
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var users = session
                    .Query<TestData, ComplexIndex>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(x => x.Identifier == "hehe3")
                    .ToList();

                Assert.Equal(1, users.Count);
            }
        }

        public class TestData
        {
            public string Name { get; set; }
            public string Second { get; set; }
            public string Identifier { get; set; }
        }
        private class ComplexIndex : AbstractIndexCreationTask<TestData>
        {
            public ComplexIndex()
            {
                Map = datas => datas.Select(i => new { Identifier = i.Identifier, Complex = new { i.Name, i.Second } });

                Index("Complex", FieldIndexing.Exact);
            }
        }
    }
}
