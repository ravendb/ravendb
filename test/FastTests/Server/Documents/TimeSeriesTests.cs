using System;
using System.Linq;
using Xunit;

namespace FastTests.Server.Documents
{
    public class TimeSeriesTests : RavenTestBase
    {
        [Fact]
        public void CanCreateSimpleTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .Single();
                    Assert.Equal(new[] { 59d }, val.Values);
                    Assert.Equal("watches/fitbit", val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp);
                }
            }
        }

        [Fact]
        public void UsingDifferentTags()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(2), "watches/apple", new[] { 70d });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(2, vals.Count);
                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp);

                    Assert.Equal(new[] { 70d }, vals[1].Values);
                    Assert.Equal("watches/apple", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp);
                }
            }
        }

        [Fact]
        public void UsingDifferentNumberOfValues_SmallToLarge()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(2), "watches/apple", new[] { 70d, 120d, 80d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(3), "watches/fitbit", new[] { 69d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(3, vals.Count);
                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp);

                    Assert.Equal(new[] { 70d, 120d, 80d }, vals[1].Values);
                    Assert.Equal("watches/apple", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp);

                    Assert.Equal(new[] { 69d }, vals[2].Values);
                    Assert.Equal("watches/fitbit", vals[2].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[2].Timestamp);
                }
            }
        }

        [Fact]
        public void UsingDifferentNumberOfValues_LargeToSmall()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "watches/apple", new[] { 70d, 120d, 80d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 59d });

            
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(3), "watches/fitbit", new[] { 69d });

                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(3, vals.Count);

                    Assert.Equal(new[] { 70d, 120d, 80d }, vals[0].Values);
                    Assert.Equal("watches/apple", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp);

                    Assert.Equal(new[] { 59d }, vals[1].Values);
                    Assert.Equal("watches/fitbit", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp);

                    Assert.Equal(new[] { 69d }, vals[2].Values);
                    Assert.Equal("watches/fitbit", vals[2].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[2].Timestamp);
                }
            }
        }




        [Fact]
        public void CanStoreAndReadMultipleTimestamps()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 61d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(3), "watches/apple-watch", new[] { 62d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(3, vals.Count);

                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp);

                    Assert.Equal(new[] { 61d }, vals[1].Values);
                    Assert.Equal("watches/fitbit", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp);

                    Assert.Equal(new[] { 62d }, vals[2].Values);
                    Assert.Equal("watches/apple-watch", vals[2].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[2].Timestamp);
                }
            }
        }

        [Fact]
        public void CanStoreLargeNumberOfValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                var offset = 0;

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {

                        for (int j = 0; j < 1000; j++)
                        {
                            session.TimeSeriesFor("users/ayende")
                                .Append("Heartrate", baseline.AddMinutes(offset++), "watches/fitbit", new double[] { offset });
                        }

                        session.SaveChanges();
                    }
                }

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(10_000, vals.Count);

                    for (int i = 0; i < 10_000; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(i), vals[i].Timestamp);
                        Assert.Equal(1 + i, vals[i].Values[0]);
                    }
                }
            }
        }


        [Fact]
        public void CanStoreValuesOutOfOrder()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                const int retries = 1000;

                var offset = 0;

                using (var session = store.OpenSession())
                {

                    for (int j = 0; j < retries; j++)
                    {
                        session.TimeSeriesFor("users/ayende")
                            .Append("Heartrate", baseline.AddMinutes(offset), "watches/fitbit", new double[] { offset });

                        offset += 5;
                    }

                    session.SaveChanges();
                }

                offset = 1;

                using (var session = store.OpenSession())
                {

                    for (int j = 0; j < retries; j++)
                    {
                        session.TimeSeriesFor("users/ayende")
                            .Append("Heartrate", baseline.AddMinutes(offset), "watches/fitbit", new double[] { offset });
                        offset += 5;
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(2 * retries, vals.Count);

                    offset = 0;
                    for (int i = 0; i < retries; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(offset), vals[i].Timestamp);
                        Assert.Equal(offset, vals[i].Values[0]);

                        offset++;
                        i++;

                        Assert.Equal(baseline.AddMinutes(offset), vals[i].Timestamp);
                        Assert.Equal(offset, vals[i].Values[0]);


                        offset += 4;
                    }
                }
            }
        }
    }
}
