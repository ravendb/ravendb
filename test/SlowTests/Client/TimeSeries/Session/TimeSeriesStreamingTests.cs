using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Session
{
    public class TimeSeriesStreamingTests : RavenTestBase
    {
        public TimeSeriesStreamingTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanStream()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.UtcNow.EnsureMilliseconds();
                using (var session = store.OpenSession())
                {

                    session.Store(new User(), "karmel");
                    var ts = session.TimeSeriesFor("karmel", "heartrate");
                    for (int i = 0; i < 10; i++)
                    {
                        ts.Append(baseline.AddMinutes(i), i,"stream");
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("karmel", "heartrate");

                    using (var it = ts.Stream())
                    {
                        var i = 0;
                        while (it.MoveNext())
                        {
                            var entry = it.Current;
                            Assert.Equal(baseline.AddMinutes(i), entry.Timestamp);
                            Assert.Equal(i, entry.Value);
                            Assert.Equal("stream", entry.Tag);
                            i++;
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task CanStreamAsync()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.UtcNow.EnsureMilliseconds();
                using (var session = store.OpenSession())
                {

                    session.Store(new User(), "karmel");
                    var ts = session.TimeSeriesFor("karmel", "heartrate");
                    for (int i = 0; i < 10; i++)
                    {
                        ts.Append(baseline.AddMinutes(i), i,"stream");
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var ts = session.TimeSeriesFor("karmel", "heartrate");
                    var it = await ts.StreamAsync();
                    await using (it)
                    {
                        var i = 0;
                        while (await it.MoveNextAsync())
                        {
                            var entry = it.Current;
                            Assert.Equal(baseline.AddMinutes(i), entry.Timestamp);
                            Assert.Equal(i, entry.Value);
                            Assert.Equal("stream", entry.Tag);
                            i++;
                        }
                    }
                }
            }
        }

        [Fact]
        public void CanStreamTyped()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.UtcNow.EnsureMilliseconds();
                using (var session = store.OpenSession())
                {

                    session.Store(new User(), "karmel");
                    var ts = session.TimeSeriesFor<TimeSeriesTypedSessionTests.HeartRateMeasure>("karmel");
                    for (int i = 0; i < 10; i++)
                    {
                        ts.Append(baseline.AddMinutes(i), new TimeSeriesTypedSessionTests.HeartRateMeasure {HeartRate = i}, "stream");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor<TimeSeriesTypedSessionTests.HeartRateMeasure>("karmel");
                    using (var it = ts.Stream())
                    {
                        var i = 0;
                        while (it.MoveNext())
                        {
                            var entry = it.Current;
                            Assert.Equal(baseline.AddMinutes(i), entry.Timestamp);
                            Assert.Equal(i, entry.Value.HeartRate);
                            Assert.Equal("stream", entry.Tag);
                            i++;
                        }
                    }
                }
            }
        }

        [Fact]
        public void CanStreamRawQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.UtcNow.EnsureMilliseconds();
                using (var session = store.OpenSession())
                {

                    session.Store(new User(), "karmel");
                    session.Store(new User(), "karmel2");
                    var ts = session.TimeSeriesFor<TimeSeriesTypedSessionTests.HeartRateMeasure>("karmel");
                    var ts2 = session.TimeSeriesFor<TimeSeriesTypedSessionTests.HeartRateMeasure>("karmel2");
                    for (int i = 0; i < 10; i++)
                    {
                        ts.Append(baseline.AddMinutes(i), new TimeSeriesTypedSessionTests.HeartRateMeasure {HeartRate = i}, "stream");
                        ts2.Append(baseline.AddMinutes(i), new TimeSeriesTypedSessionTests.HeartRateMeasure {HeartRate = i * 2}, "stream");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    string tsQueryText = @"
from HeartRateMeasures
group by '5 min'
select last()
";
                    var query = session.Advanced.DocumentQuery<User>()
                        .SelectTimeSeries(builder => builder.Raw<TimeSeriesAggregationResult<TimeSeriesTypedSessionTests.HeartRateMeasure>>(tsQueryText));

                    using (var docStream = session.Advanced.Stream(query))
                    {
                        while (docStream.MoveNext())
                        {
                            var document = docStream.Current.Result;
                            var timeseries = document.Stream;
                            while (timeseries.MoveNext())
                            {
                                var entry = timeseries.Current;
                                var x = entry.Last.HeartRate;
                            }
                        }
                    }

                    var query1 = session.Advanced.DocumentQuery<User>()
                        .SelectTimeSeries(builder => builder.Raw<TimeSeriesAggregationResult>(tsQueryText));

                    using (var docStream = session.Advanced.Stream(query1))
                    {
                        while (docStream.MoveNext())
                        {
                            var document = docStream.Current.Result;
                            var timeseries = document.Stream;
                            while (timeseries.MoveNext())
                            {
                                var entry = timeseries.Current;
                                var x = entry.Last[0];
                            }
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task CanStreamRawQueryAsync()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                using (var session = store.OpenSession())
                {

                    session.Store(new User(), "karmel");
                    session.Store(new User(), "karmel2");
                    var ts = session.TimeSeriesFor<TimeSeriesTypedSessionTests.HeartRateMeasure>("karmel");
                    var ts2 = session.TimeSeriesFor<TimeSeriesTypedSessionTests.HeartRateMeasure>("karmel2");
                    for (int i = 0; i < 10; i++)
                    {
                        ts.Append(baseline.AddMinutes(i), new TimeSeriesTypedSessionTests.HeartRateMeasure {HeartRate = i}, "stream");
                        ts2.Append(baseline.AddMinutes(i), new TimeSeriesTypedSessionTests.HeartRateMeasure {HeartRate = i * 2}, "stream");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    string tsQueryText = @"
from HeartRateMeasures
group by '5 min'
select last()
";
                    var query = session.Advanced.AsyncDocumentQuery<User>()
                        .SelectTimeSeries(builder => builder.Raw<TimeSeriesAggregationResult<TimeSeriesTypedSessionTests.HeartRateMeasure>>(tsQueryText));

                    var docCount = 0;
                    var docStream = await session.Advanced.StreamAsync(query);
                    await using (docStream)
                    {
                        while (await docStream.MoveNextAsync())
                        {
                            docCount++;
                            var tsCount = 0;
                            var document = docStream.Current.Result;
                            var timeseries = document.StreamAsync;
                            while (await timeseries.MoveNextAsync())
                            {
                                var entry = timeseries.Current;

                                Assert.Equal((tsCount * 5 + 4) * docCount, entry.Last.HeartRate);
                                tsCount++;
                            }
                        }
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesUsingDocumentQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                store.TimeSeries.Register<User>("Heartrate", new[] {"BPM"});
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), 59, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), 79, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), 69, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), 159, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), 179, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), 169, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    string tsQueryText = @"
from Heartrate between $start and $end
where Tag = 'watches/fitbit'
group by '1 month'
select min(), max(), avg()
";

                    var query = session.Advanced.DocumentQuery<User>()
                    .WhereGreaterThan(u => u.Age, 21)
                    .SelectTimeSeries(builder => builder.Raw<TimeSeriesAggregationResult>(tsQueryText))
                    .AddParameter("start", baseline.EnsureUtc())
                    .AddParameter("end", baseline.AddMonths(3).EnsureUtc());
                    using (var streamResults = session.Advanced.Stream(query))
                    {
                        while (streamResults.MoveNext())
                        {
                            var result = streamResults.Current.Result;
                            var stream = result.Stream;
                            
                            Assert.True(stream.MoveNext());
                            var entry = stream.Current;
                            Assert.Equal(69, entry.Max[0]);
                            Assert.Equal(59, entry.Min[0]);
                            Assert.Equal(64, entry.Average[0]);

                            Assert.True(stream.MoveNext());
                            entry = stream.Current;
                            Assert.Equal(169, entry.Max[0]);
                            Assert.Equal(169, entry.Min[0]);
                            Assert.Equal(169, entry.Average[0]);
                            
                            Assert.False(stream.MoveNext());
                        }
                    }
                }
            }
        }
    }
}
