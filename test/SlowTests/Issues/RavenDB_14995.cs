using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes.TimeSeries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14995 : RavenTestBase
    {
        public RavenDB_14995(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldWork_TimeSeries()
        {
            const int numberOfCompanies = 77;
            const int numberOfTimeSeriesEntriesPerCompany = 333;

            var now = DateTime.UtcNow;

            using (var store = GetDocumentStore(new Options { ModifyDocumentStore = s => s.Conventions.MaxNumberOfRequestsPerSession = int.MaxValue }))
            {
                new SimpleTimeSeriesMapReduce().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < numberOfCompanies; i++)
                    {
                        var company = new Company { Name = $"C_{i}" };
                        session.Store(company, $"companies/{i}");

                        var companyTimeSeries = session.TimeSeriesFor(company, "StockPrice");

                        for (var j = 0; j < numberOfTimeSeriesEntriesPerCompany; j++)
                        {
                            companyTimeSeries.Append(now.AddMinutes(j), j);
                        }

                        session.SaveChanges();
                    }
                }

                for (var i = 0; i < numberOfCompanies; i++)
                {
                    Indexes.WaitForIndexing(store);
                    RavenTestHelper.AssertNoIndexErrors(store);
                    AssertCompanies(numberOfCompanies - i);

                    using (var session = store.OpenSession())
                    {
                        session.Delete($"companies/{i}");
                        session.SaveChanges();
                    }
                }

                void AssertCompanies(int count)
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session
                            .Query<SimpleTimeSeriesMapReduce.Result, SimpleTimeSeriesMapReduce>()
                            .ToList();

                        Assert.Equal(count, results.Count);

                        foreach (var result in results)
                        {
                            Assert.NotNull(result.DocumentId);
                            Assert.Equal(numberOfTimeSeriesEntriesPerCompany, result.Count);
                        }
                    }
                }
            }
        }

        private class SimpleTimeSeriesMapReduce : AbstractTimeSeriesIndexCreationTask<Company, SimpleTimeSeriesMapReduce.Result>
        {
            public class Result
            {
                public string DocumentId { get; set; }

                public int Count { get; set; }
            }

            public SimpleTimeSeriesMapReduce()
            {
                AddMap("StockPrice", segments => from segment in segments
                                                 from entry in segment.Entries
                                                 select new Result
                                                 {
                                                     DocumentId = segment.DocumentId,
                                                     Count = 1
                                                 });

                Reduce = results => from result in results
                                    group result by result.DocumentId into g
                                    select new Result
                                    {
                                        DocumentId = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }
    }
}
