using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Session
{
    public class TimeSeriesTypedSessionTests : RavenTestBase
    {
        public TimeSeriesTypedSessionTests(ITestOutputHelper output) : base(output)
        {
        }

        public class HeartRateMeasure : TimeSeriesEntry
        {
            public HeartRateMeasure()
            {
                Values = new double[1];
            }

            public double HeartRate { get => Values[0]; set => Values[0] = value; }
        }

        public class StockPrice : TimeSeriesEntry
        {
            public StockPrice()
            {
                Values = new double[5];
            }

            public double Open { get => Values[0]; set => Values[0] = value; }
            public double Close { get => Values[1]; set => Values[1] = value; }
            public double High { get => Values[2]; set => Values[2] = value; }
            public double Low { get => Values[3]; set => Values[3] = value; }
            public double Volume { get => Values[4]; set => Values[4] = value; }
        }

        [Fact]
        public void CanCreateSimpleTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var measure = new HeartRateMeasure
                    {
                        Timestamp = baseline.AddMinutes(1),
                        HeartRate = 59d,
                        Tag = "watches/fitbit"
                    };
                    session.TimeSeriesFor<HeartRateMeasure>("users/ayende", "Heartrate").Append(measure);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor<HeartRateMeasure>("users/ayende", "Heartrate")
                        .Get().Single();

                    Assert.Equal(59d , val.HeartRate);
                    Assert.Equal("watches/fitbit", val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanCreateSimpleTimeSeries2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                    tsf.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 60d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 61d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor<HeartRateMeasure>("users/ayende", "Heartrate")
                        .Get()
                        .ToList();
                    Assert.Equal(2, val.Count);
                }
            }
        }

        [Fact]
        public void CanRequestNonExistingTimeSeriesRange()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline, new[] { 58d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(10), new[] { 60d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor<HeartRateMeasure>("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(-10), baseline.AddMinutes(-5))?
                        .ToList();

                    Assert.Null(vals);

                    vals = session.TimeSeriesFor<HeartRateMeasure>("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(5), baseline.AddMinutes(9))?
                        .ToList();

                    Assert.Null(vals);
                }
            }
        }

        [Fact]
        public void CanGetTimeSeriesNames()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/karmel");
                    session.TimeSeriesFor<HeartRateMeasure>("users/karmel", "Heartrate")
                        .Append(new HeartRateMeasure
                        {
                            HeartRate = 66,
                            Tag = "MyHeart",
                            Timestamp = DateTime.Now
                        });

                    session.TimeSeriesFor<StockPrice>("users/karmel", "Nasdaq")
                        .Append(new StockPrice
                        {
                            Open = 66,
                            Close = 55,
                            High = 113.4,
                            Low = 52.4,
                            Volume = 15472,
                            Timestamp = DateTime.Now
                        });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/karmel");
                    var tsNames = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(2, tsNames.Count);

                    // should be sorted
                    Assert.Equal("Heartrate", tsNames[0]);
                    Assert.Equal("Nasdaq", tsNames[1]);

                    var heartRateMeasures = session.TimeSeriesFor<HeartRateMeasure>(user, "Heartrate").Get().Single();
                    Assert.Equal(66, heartRateMeasures.HeartRate);

                    var stockPrice = session.TimeSeriesFor<StockPrice>(user, "Nasdaq").Get().Single();
                    Assert.Equal(66, stockPrice.Open);
                    Assert.Equal(55, stockPrice.Close);
                    Assert.Equal(113.4, stockPrice.High);
                    Assert.Equal(52.4, stockPrice.Low);
                    Assert.Equal(15472, stockPrice.Volume);
                }
            }
        }
    }
}
