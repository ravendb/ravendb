using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3136 : RavenTestBase
    {
        [Fact]
        public void AggregateByIntegerShouldReturnResultWithValuesAndCountEvenWithLambdaExpression()
        {
            using (var store = GetDocumentStore())
            {
                new SampleData_Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new SampleData { IntegerAge = 2, StringAge = "2" });
                    session.Store(new SampleData { IntegerAge = 2, StringAge = "2" });
                    session.Store(new SampleData { IntegerAge = 3, StringAge = "3" });
                    session.SaveChanges();
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var resultInteger =
                        session.Query<SampleData, SampleData_Index>()
                            .AggregateBy(x => x.ByField(y => y.IntegerAge))
                            .Execute();

                    Assert.Equal(resultInteger.Count, 1);
                    Assert.Equal(resultInteger.First().Value.Values.Count(), 2);
                    Assert.Equal(GetFirstSortedRangeString(resultInteger), "2");
                }
            }
        }

        private static string GetFirstSortedRangeString(Dictionary<string, FacetResult> resultInteger)
        {
            var sorted = resultInteger.First().Value.Values.Select(valueValue => valueValue.Range).ToList();
            sorted.Sort();
            return sorted.First();
        }

        [Fact]
        public void AggregateByIntegerShouldReturnResultWithValuesAndCount()
        {
            using (var store = GetDocumentStore())
            {
                new SampleData_Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new SampleData { IntegerAge = 2, StringAge = "2" });
                    session.Store(new SampleData { IntegerAge = 2, StringAge = "2" });
                    session.Store(new SampleData { IntegerAge = 3, StringAge = "3" });
                    session.SaveChanges();
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var resultInteger =
                        session.Query<SampleData, SampleData_Index>()
                            .AggregateBy(x => x.ByField("IntegerAge").MinOn(y => y.IntegerAge))
                            .Execute();

                    Assert.Equal(resultInteger.Count, 1);
                    Assert.Equal(resultInteger.First().Value.Values.Count(), 2);
                    Assert.Equal(GetFirstSortedRangeString(resultInteger), "2");
                }
            }
        }

        [Fact]
        public void AggregateByStringShouldReturnResultWithValuesAndCount()
        {
            using (var store = GetDocumentStore())
            {
                new SampleData_Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new SampleData { IntegerAge = 2, StringAge = "2" });
                    session.Store(new SampleData { IntegerAge = 2, StringAge = "2" });
                    session.Store(new SampleData { IntegerAge = 3, StringAge = "3" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var resultString =
                        session.Query<SampleData, SampleData_Index>()
                            .AggregateBy(x => x.ByField(y => y.StringAge))
                            .Execute();

                    Assert.Equal(resultString.Count, 1);
                    Assert.Equal(resultString.First().Value.Values.Count(), 2);
                    Assert.Equal(GetFirstSortedRangeString(resultString), "2");
                }
            }
        }

        private class SampleData
        {
            public string StringAge { get; set; }
            public int IntegerAge { get; set; }
        }

        private class SampleData_Index : AbstractIndexCreationTask<SampleData>
        {
            public SampleData_Index()
            {
                Map = docs => from doc in docs select new { doc.StringAge, doc.IntegerAge };
                TermVector(x => x.IntegerAge, FieldTermVector.Yes);
                TermVector(x => x.StringAge, FieldTermVector.Yes);
            }
        }
    }
}


