using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes.TimeSeries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14917 : RavenTestBase
    {
        public RavenDB_14917(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUseSegmentSummaryInIndex()
        {
            using (var store = GetDocumentStore())
            {
                new TimeSeriesIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var company = new Company();

                    session.Store(company);

                    session.TimeSeriesFor(company, "TS").Append(DateTime.Now, new[] { 3, 5.5 });
                    session.TimeSeriesFor(company, "TS").Append(DateTime.Now.AddMilliseconds(10), new[] { 2, 3.5 });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Query<TimeSeriesIndex.Result, TimeSeriesIndex>()
                        .ToList();

                    Assert.Equal(1, results.Count);

                    var result = results[0];

                    Assert.Equal(new[] { 2, 3.5 }, result.Min);
                    Assert.Equal(new double[] { 5, 9 }, result.Sum);
                    Assert.Equal(new [] { 3, 5.5 }, result.Max);
                    Assert.Equal(2, result.Count);
                    Assert.Equal(3, result.FirstMax);
                    Assert.Equal(3.5, result.LastMin);
                }
            }
        }

        private class TimeSeriesIndex : AbstractTimeSeriesIndexCreationTask<Company>
        {
            public class Result
            {
                public string Name { get; set; }

                public int Count { get; set; }

                public double[] Min { get; set; }

                public double[] Max { get; set; }

                public double[] Sum { get; set; }

                public double LastMin { get; set; }

                public double FirstMax { get; set; }
            }

            public TimeSeriesIndex()
            {
                AddMap("TS", segments => from segment in segments
                                         select new Result
                                         {
                                             Name = segment.Name,
                                             Count = segment.Count,
                                             FirstMax = segment.Max[0],
                                             LastMin = segment.Min.Last(),
                                             Max = segment.Max,
                                             Min = segment.Min,
                                             Sum = segment.Sum
                                         });
            }
        }
    }
}
