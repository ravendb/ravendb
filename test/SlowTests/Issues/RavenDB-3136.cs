using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
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
                            .AggregateBy(x => x.IntegerAge)
                            .CountOn(x => x.IntegerAge)
                            .ToList();

                    Assert.Equal(resultInteger.Results.Count, 1);
                    Assert.Equal(resultInteger.Results.First().Value.Values.Count(), 2);
                    var sorted = resultInteger.Results.First().Value.Values.Select(valueValue => valueValue.Range).ToList();
                    sorted.Sort();
                    Assert.Equal(sorted.First(), "2");
                }
            }
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
                            .AggregateBy("IntegerAge")
                            .CountOn(x => x.IntegerAge)
                            .ToList();

                    Assert.Equal(resultInteger.Results.Count, 1);
                    Assert.Equal(resultInteger.Results.First().Value.Values.Count(), 2);
                    Assert.Equal(resultInteger.Results.First().Value.Values.First().Range, "2");
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
                            .AggregateBy(x => x.StringAge)
                            .CountOn(x => x.StringAge)
                            .ToList();

                    Assert.Equal(resultString.Results.Count, 1);
                    Assert.Equal(resultString.Results.First().Value.Values.Count(), 2);
                    Assert.Equal(resultString.Results.First().Value.Values.First().Range, "2");
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
                Sort(x => x.IntegerAge, SortOptions.Numeric);
                TermVector(x => x.IntegerAge, FieldTermVector.Yes);
                TermVector(x => x.StringAge, FieldTermVector.Yes);
            }
        }
    }
}


