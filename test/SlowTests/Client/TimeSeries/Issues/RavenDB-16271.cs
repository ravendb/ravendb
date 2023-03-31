using System;
using FastTests;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_16271 : RavenTestBase
    {
        public RavenDB_16271(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanQueryTimeSeriesWithQuotedName(Options options)
        {
            const string documentId = "users/ayende";
            const string seriesName = "1-0.16.7.1";

            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    var tsf = session.TimeSeriesFor(documentId, seriesName);
                    tsf.Append(DateTime.Now, 1);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // test without alias 
                    var qurey = session.Advanced.RawQuery<TimeSeriesRawResult>($"from Users select timeseries(from '{seriesName}')");
                    var result = qurey.First();

                    Assert.Equal(1, result.Count);
                }

                using (var session = store.OpenSession())
                {
                    // test with alias 
                    var qurey = session.Advanced.RawQuery<TimeSeriesRawResult>($"from Users as u select timeseries(from u.'{seriesName}')");
                    var result = qurey.First();

                    Assert.Equal(1, result.Count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanQueryTimeSeriesWithQuotedName2(Options options)
        {
            const string documentId = "users/ayende";
            const string seriesName = "'1.2-345.6'";

            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    var tsf = session.TimeSeriesFor(documentId, seriesName);
                    tsf.Append(DateTime.Now, 1);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // test without alias 
                    var qurey = session.Advanced.RawQuery<TimeSeriesRawResult>($"from Users select timeseries(from {seriesName})");
                    var result = qurey.First();

                    Assert.Equal(1, result.Count);
                }

                using (var session = store.OpenSession())
                {
                    // test with alias 
                    var qurey = session.Advanced.RawQuery<TimeSeriesRawResult>($"from Users as u select timeseries(from u.{seriesName})");
                    var result = qurey.First();

                    Assert.Equal(1, result.Count);
                }
            }
        }

    }
}
