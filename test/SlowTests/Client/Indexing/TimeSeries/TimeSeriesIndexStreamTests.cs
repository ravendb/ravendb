using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Indexing.TimeSeries
{
    public class TimeSeriesIndexStreamTests : RavenTestBase
    {
        public TimeSeriesIndexStreamTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void BasicMapIndex()
        {
            using (var store = GetDocumentStore())
            {
                var now1 = RavenTestHelper.UtcToday;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    var ts = session.TimeSeriesFor(company, "HeartRate");

                    for (int i = 0; i < 10; i++)
                    {
                        ts.Append(now1.AddMinutes(i), new double[] {i}, "tag");
                    }

                    session.SaveChanges();
                }

                var timeSeriesIndex = new MyTsIndex();
                timeSeriesIndex.Execute(store);
                Indexes.WaitForIndexing(store);
               
                using (var session = store.OpenSession())
                {
                    var i = 0;
                    using var stream = session.Advanced.Stream(session.Query<MyTsIndex.Result, MyTsIndex>());
                    while (stream.MoveNext())
                    {
                        var results = stream.Current.Document;
                        Assert.Equal(now1.AddMinutes(i), results.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(i, results.HeartBeat);
                        Assert.Equal("companies/1", results.User);
                        i++;
                    }

                    Assert.Equal(10, i);
                }
            }
        }

        private class MyTsIndex : AbstractTimeSeriesIndexCreationTask<Company>
        {
            public class Result
            {
                public double HeartBeat { get; set; }

                public DateTime Timestamp { get; set; }

                public string User{ get; set; }
            }

            public MyTsIndex()
            {
                AddMap(
                    "HeartRate",
                    timeSeries => from ts in timeSeries
                        from entry in ts.Entries
                        select new
                        {
                            HeartBeat = entry.Values[0],
                            entry.Timestamp,
                            User = ts.DocumentId
                        });
            }
        }
    }
}
