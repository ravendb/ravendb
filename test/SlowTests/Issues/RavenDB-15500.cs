using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15500 : RavenTestBase
    {
        public RavenDB_15500(ITestOutputHelper output) : base(output)
        {
        }

        private class TestDocument
        {
            public decimal Average;
            public string Name;
#pragma warning disable 649
            public decimal? Specific;
#pragma warning restore 649
        }

        private class AverageIndex : AbstractIndexCreationTask<TestDocument, AverageIndex.Result>
        {
            public AverageIndex()
            {
                Map = documents => from document in documents
                                   select new { document.Name, Average = document.Specific ?? document.Average };

                StoreAllFields(FieldStorage.Yes);
            }

            public class Result
            {
                public string Name { get; set; }
                public decimal Average { get; set; }
            }
        }

        [Fact]
        public void CanProjectDecimalFromIndex()
        {
            using (DocumentStore store = GetDocumentStore())
            {
                store.ExecuteIndex(new AverageIndex());

                //var average = 0.0001m; // WORKING
                decimal average = 0.00001m; // NOT WORKING

                using (IDocumentSession session = store.OpenSession())
                {
                    session.Store(new TestDocument { Name = "Document 1", Average = average });
                    session.SaveChanges();
                }

                WaitForIndexing(store);
                //  WaitForUserToContinueTheTest(store);
                using (IDocumentSession session = store.OpenSession())
                {
                    //from index 'AverageIndex' select Name, Average
                    AverageIndex.Result result = session.Query<AverageIndex.Result, AverageIndex>().ProjectInto<AverageIndex.Result>().First();

                    Assert.Equal(average, result.Average);
                }
            }
        }
    }
}
