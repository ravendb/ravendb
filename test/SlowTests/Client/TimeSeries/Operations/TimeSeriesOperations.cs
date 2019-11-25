using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Documents;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Client.TimeSeries.Query;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Operations
{
    public class TimeSeriesOperations : RavenTestBase
    {
        public TimeSeriesOperations(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCreateAndGetSimpleTimeSeriesUsingStoreOperations()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.SaveChanges();
                }

                var baseline = DateTime.Today;

                var timeSeriesOp = new DocumentTimeSeriesOperation
                {
                    Id = "users/ayende",
                    Appends = new List<AppendTimeSeriesOperation>()
                    {
                        new AppendTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            Tag = "watches/fitbit",
                            Timestamp = baseline.AddSeconds(1),
                            Values = new[]
                            {
                                59d
                            }
                        }
                    }
                };

                var timeSeriesBatch = new TimeSeriesBatchOperation(timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var timesSeriesDetails = store.Operations.Send(
                    new GetTimeSeriesOperation("users/ayende", "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal("users/ayende", timesSeriesDetails.Id);
                Assert.Equal(1, timesSeriesDetails.Values.Count);
                Assert.Equal(1, timesSeriesDetails.Values["Heartrate"][0].Values.Length);

                var value = timesSeriesDetails.Values["Heartrate"][0].Values[0];

                Assert.Equal(59d, value.Values[0]);
                Assert.Equal("watches/fitbit", value.Tag);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp);
            }
        }

        [Fact]
        public void CanStoreAndReadMultipleTimestampsUsingStoreOperations()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.SaveChanges();
                }

                var baseline = DateTime.Today;

                var timeSeriesOp = new DocumentTimeSeriesOperation
                {
                    Id = "users/ayende",
                    Appends = new List<AppendTimeSeriesOperation>()
                    {
                        new AppendTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            Tag = "watches/fitbit",
                            Timestamp = baseline.AddSeconds(1),
                            Values = new[]
                            {
                                59d
                            }
                        },
                        new AppendTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            Tag = "watches/fitbit",
                            Timestamp = baseline.AddSeconds(2),
                            Values = new[]
                            {
                                61d
                            }
                        },
                        new AppendTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            Tag = "watches/apple-watch",
                            Timestamp = baseline.AddSeconds(5),
                            Values = new[]
                            {
                                60d
                            }
                        }
                    }
                };

                var timeSeriesBatch = new TimeSeriesBatchOperation(timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var timesSeriesDetails = store.Operations.Send(
                    new GetTimeSeriesOperation("users/ayende", "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal("users/ayende", timesSeriesDetails.Id);
                Assert.Equal(1, timesSeriesDetails.Values.Count);
                Assert.Equal(3, timesSeriesDetails.Values["Heartrate"][0].Values.Length);

                var value = timesSeriesDetails.Values["Heartrate"][0].Values[0];

                Assert.Equal(59d, value.Values[0]);
                Assert.Equal("watches/fitbit", value.Tag);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp);

                value = timesSeriesDetails.Values["Heartrate"][0].Values[1];

                Assert.Equal(61d, value.Values[0]);
                Assert.Equal("watches/fitbit", value.Tag);
                Assert.Equal(baseline.AddSeconds(2), value.Timestamp);


                value = timesSeriesDetails.Values["Heartrate"][0].Values[2];

                Assert.Equal(60d, value.Values[0]);
                Assert.Equal("watches/apple-watch", value.Tag);
                Assert.Equal(baseline.AddSeconds(5), value.Timestamp);
            }
        }

        [Fact]
        public void CanDeleteTimestampUsingStoreOperations()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.SaveChanges();
                }

                var baseline = DateTime.Today;

                var timeSeriesOp = new DocumentTimeSeriesOperation
                {
                    Id = "users/ayende",
                    Appends = new List<AppendTimeSeriesOperation>()
                    {
                        new AppendTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            Tag = "watches/fitbit",
                            Timestamp = baseline.AddSeconds(1),
                            Values = new[]
                            {
                                59d
                            }
                        },
                        new AppendTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            Tag = "watches/fitbit",
                            Timestamp = baseline.AddSeconds(2),
                            Values = new[]
                            {
                                61d
                            }
                        },
                        new AppendTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            Tag = "watches/fitbit",
                            Timestamp = baseline.AddSeconds(3),
                            Values = new[]
                            {
                                60d
                            }
                        },
                        new AppendTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            Tag = "watches/fitbit",
                            Timestamp = baseline.AddSeconds(4),
                            Values = new[]
                            {
                                62.5d
                            }
                        },
                        new AppendTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            Tag = "watches/fitbit",
                            Timestamp = baseline.AddSeconds(5),
                            Values = new[]
                            {
                                62d
                            }
                        }
                    }
                };

                var timeSeriesBatch = new TimeSeriesBatchOperation(timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var timesSeriesDetails = store.Operations.Send(
                    new GetTimeSeriesOperation("users/ayende", "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal("users/ayende", timesSeriesDetails.Id);
                Assert.Equal(1, timesSeriesDetails.Values.Count);
                Assert.Equal(5, timesSeriesDetails.Values["Heartrate"][0].Values.Length);


                timeSeriesOp = new DocumentTimeSeriesOperation
                {
                    Id = "users/ayende",
                    Removals = new List<RemoveTimeSeriesOperation>()
                    {
                        new RemoveTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            From = baseline.AddSeconds(2),
                            To = baseline.AddSeconds(3)
                        }
                    }
                };

                timeSeriesBatch = new TimeSeriesBatchOperation(timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                timesSeriesDetails = store.Operations.Send(
                    new GetTimeSeriesOperation("users/ayende", "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal("users/ayende", timesSeriesDetails.Id);
                Assert.Equal(1, timesSeriesDetails.Values.Count);
                Assert.Equal(3, timesSeriesDetails.Values["Heartrate"][0].Values.Length);

                var value = timesSeriesDetails.Values["Heartrate"][0].Values[0];
                Assert.Equal(59d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp);

                value = timesSeriesDetails.Values["Heartrate"][0].Values[1];
                Assert.Equal(62.5d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(4), value.Timestamp);

                value = timesSeriesDetails.Values["Heartrate"][0].Values[2];
                Assert.Equal(62d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(5), value.Timestamp);

            }


            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 69d });
                    session.TimeSeriesFor("users/ayende")
                     .Append("Heartrate", baseline.AddMinutes(3), "watches/fitbit", new[] { 79d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Remove("Heartrate", baseline.AddMinutes(2));

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

                    Assert.Equal(new[] { 79d }, vals[1].Values);
                    Assert.Equal("watches/fitbit", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[1].Timestamp);
                }
            }
        }

        [Fact]
        public void CanDeleteLargeRange()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {

                    session.Store(new User(), "foo/bar");
                    var tsf = session.TimeSeriesFor("foo/bar");

                    for (int j = 1; j < 10_000; j++)
                    {
                        var offset = j * 10;
                        var time = baseline.AddSeconds(offset);
                        
                        tsf.Append("BloodPressure", time, "watches/apple", new[] { (double)(j) });
                    }

                    session.SaveChanges();
                }
                var rawQuery = @"
                                declare timeseries blood_pressure(doc) 
                                {
                                    from doc.BloodPressure between $start and $end 
                                    group by 1h 
                                    select min(), max(), avg(), first(), last()
                                }
                                from Users as p 
                                select blood_pressure(p) as BloodPressure
                                ";

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesQueryTests.RawQueryResult>(rawQuery)
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);

                    var agg = result[0];

                    var bloodPressure = agg.BloodPressure;
                    var count = bloodPressure.Results.Sum(r => r.Count[0]);
                    Assert.Equal(8640, count);
                    Assert.Equal(bloodPressure.Count, count);
                    Assert.Equal(bloodPressure.Results.Length, 24);

                    for (var index = 0; index < bloodPressure.Results.Length; index++)
                    {
                        var item = bloodPressure.Results[index];
                        Assert.Equal(360, item.Count[0]);
                        Assert.Equal(index * 360 + 180 + 0.5, item.Avg[0]);
                        Assert.Equal((index + 1) * 360, item.Max[0]);
                        Assert.Equal(index * 360 + 1, item.Min[0]);
                        Assert.Equal(index * 360 + 1, item.First[0]);
                        Assert.Equal((index + 1) * 360, item.Last[0]);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("foo/bar");
                    tsf.Remove("BloodPressure", baseline.AddSeconds(3600), baseline.AddSeconds(3600 * 10)); // remove 9 hours
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoCaching = true
                }))
                {
                    var query = session.Advanced.RawQuery<TimeSeriesQueryTests.RawQueryResult>(rawQuery)
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var result = query.ToList();

                    var agg = result[0];

                    var bloodPressure = agg.BloodPressure;
                    var count = bloodPressure.Results.Sum(r => r.Count[0]);
                    Assert.Equal(5399, count);
                    Assert.Equal(bloodPressure.Count, count);
                    Assert.Equal(bloodPressure.Results.Length, 15);

                    var index = 0;
                    var item = bloodPressure.Results[index];
                    Assert.Equal(359, item.Count[0]);
                    Assert.Equal(180, item.Avg[0]);
                    Assert.Equal(359, item.Max[0]);
                    Assert.Equal(1, item.Min[0]);
                    Assert.Equal(1, item.First[0]);
                    Assert.Equal(359, item.Last[0]);

                    for (index = 1; index < bloodPressure.Results.Length; index++)
                    {
                        item = bloodPressure.Results[index];
                        var realIndex = index + 9;

                        Assert.Equal(360, item.Count[0]);
                        Assert.Equal(realIndex * 360 + 180 + 0.5, item.Avg[0]);
                        Assert.Equal((realIndex + 1) * 360, item.Max[0]);
                        Assert.Equal(realIndex * 360 + 1, item.Min[0]);
                        Assert.Equal(realIndex * 360 + 1, item.First[0]);
                        Assert.Equal((realIndex + 1) * 360, item.Last[0]);
                    }
                }
            }
        }

        [Fact]
        public void CanAppendAndRemoveTimestampsInSingleBatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.SaveChanges();
                }

                var baseline = DateTime.Today;

                var timeSeriesOp = new DocumentTimeSeriesOperation
                {
                    Id = "users/ayende",
                    Appends = new List<AppendTimeSeriesOperation>()
                    {
                        new AppendTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            Tag = "watches/fitbit",
                            Timestamp = baseline.AddSeconds(1),
                            Values = new[]
                            {
                                59d
                            }
                        },
                        new AppendTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            Tag = "watches/fitbit",
                            Timestamp = baseline.AddSeconds(2),
                            Values = new[]
                            {
                                61d
                            }
                        },
                        new AppendTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            Tag = "watches/fitbit",
                            Timestamp = baseline.AddSeconds(3),
                            Values = new[]
                            {
                                61.5d
                            }
                        }
                    }
                };

                var timeSeriesBatch = new TimeSeriesBatchOperation(timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var timesSeriesDetails = store.Operations.Send(
                    new GetTimeSeriesOperation("users/ayende", "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal("users/ayende", timesSeriesDetails.Id);
                Assert.Equal(1, timesSeriesDetails.Values.Count);
                Assert.Equal(3, timesSeriesDetails.Values["Heartrate"][0].Values.Length);

                timeSeriesOp = new DocumentTimeSeriesOperation
                {
                    Id = "users/ayende",
                    Appends = new List<AppendTimeSeriesOperation>()
                    {
                        new AppendTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            Tag = "watches/fitbit",
                            Timestamp = baseline.AddSeconds(4),
                            Values = new[]
                            {
                                60d
                            }
                        },
                        new AppendTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            Tag = "watches/fitbit",
                            Timestamp = baseline.AddSeconds(5),
                            Values = new[]
                            {
                                62.5d
                            }
                        },
                        new AppendTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            Tag = "watches/fitbit",
                            Timestamp = baseline.AddSeconds(6),
                            Values = new[]
                            {
                                62d
                            }
                        }
                    },
                    Removals = new List<RemoveTimeSeriesOperation>
                    {
                        new RemoveTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            From = baseline.AddSeconds(2),
                            To = baseline.AddSeconds(3)
                        }
                    }
                };

                timeSeriesBatch = new TimeSeriesBatchOperation(timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                timesSeriesDetails = store.Operations.Send(
                    new GetTimeSeriesOperation("users/ayende", "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal("users/ayende", timesSeriesDetails.Id);
                Assert.Equal(1, timesSeriesDetails.Values.Count);
                Assert.Equal(4, timesSeriesDetails.Values["Heartrate"][0].Values.Length);

                var value = timesSeriesDetails.Values["Heartrate"][0].Values[0];
                Assert.Equal(59d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp);

                value = timesSeriesDetails.Values["Heartrate"][0].Values[1];
                Assert.Equal(60d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(4), value.Timestamp);

                value = timesSeriesDetails.Values["Heartrate"][0].Values[2];
                Assert.Equal(62.5d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(5), value.Timestamp);

                value = timesSeriesDetails.Values["Heartrate"][0].Values[3];
                Assert.Equal(62d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(6), value.Timestamp);

            }


            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 69d });
                    session.TimeSeriesFor("users/ayende")
                     .Append("Heartrate", baseline.AddMinutes(3), "watches/fitbit", new[] { 79d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Remove("Heartrate", baseline.AddMinutes(2));

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

                    Assert.Equal(new[] { 79d }, vals[1].Values);
                    Assert.Equal("watches/fitbit", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[1].Timestamp);
                }
            }
        }

        [Fact]
        public void ShouldThrowOnAttemptToCreateTimeSeriesOnMissingDocument()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                var timeSeriesOp = new DocumentTimeSeriesOperation
                {
                    Id = "users/ayende",
                    Appends = new List<AppendTimeSeriesOperation>()
                    {
                        new AppendTimeSeriesOperation
                        {
                            Name = "Heartrate",
                            Tag = "watches/fitbit",
                            Timestamp = baseline.AddSeconds(1),
                            Values = new[] {59d}
                        }
                    }
                };

                var timeSeriesBatch = new TimeSeriesBatchOperation(timeSeriesOp);

                var ex = Assert.Throws<DocumentDoesNotExistException>(() => store.Operations.Send(timeSeriesBatch));

                Assert.Contains("Cannot operate on time series of a missing document", ex.Message);

            }
        }

        [Fact]
        public void CanGetMultipleRangesInSingleRequest()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.SaveChanges();
                }

                var baseline = DateTime.Today;

                var timeSeriesOp = new DocumentTimeSeriesOperation
                {
                    Id = "users/ayende",
                    Appends = new List<AppendTimeSeriesOperation>()
                };

                for (int i = 0; i <= 360; i++)
                {
                    timeSeriesOp.Appends.Add(new AppendTimeSeriesOperation
                    {
                        Name = "Heartrate",
                        Tag = "watches/fitbit",
                        Timestamp = baseline.AddSeconds(i * 10),
                        Values = new[] { 59d }
                    });
                }

                var timeSeriesBatch = new TimeSeriesBatchOperation(timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var timesSeriesDetails = store.Operations.Send(
                    new GetTimeSeriesOperation("users/ayende", "Heartrate", new List<TimeSeriesRange>
                    {
                        new TimeSeriesRange
                        {
                            From = baseline.AddMinutes(5),
                            To = baseline.AddMinutes(10)
                        },

                        new TimeSeriesRange
                        {
                            From = baseline.AddMinutes(15),
                            To = baseline.AddMinutes(30)
                        },

                        new TimeSeriesRange
                        {
                            From = baseline.AddMinutes(40),
                            To = baseline.AddMinutes(60)
                        }
                    }));

                Assert.Equal("users/ayende", timesSeriesDetails.Id);
                Assert.Equal(1, timesSeriesDetails.Values.Count);
                Assert.Equal(3, timesSeriesDetails.Values["Heartrate"].Count);

                var range = timesSeriesDetails.Values["Heartrate"][0];

                Assert.Equal(baseline.AddMinutes(5), range.From);
                Assert.Equal(baseline.AddMinutes(10), range.To);

                Assert.Equal(31, range.Values.Length);
                Assert.Equal(baseline.AddMinutes(5), range.Values[0].Timestamp);
                Assert.Equal(baseline.AddMinutes(10), range.Values[30].Timestamp);

                range = timesSeriesDetails.Values["Heartrate"][1];

                Assert.Equal(baseline.AddMinutes(15), range.From);
                Assert.Equal(baseline.AddMinutes(30), range.To);

                Assert.Equal(91, range.Values.Length);
                Assert.Equal(baseline.AddMinutes(15), range.Values[0].Timestamp);
                Assert.Equal(baseline.AddMinutes(30), range.Values[90].Timestamp);

                range = timesSeriesDetails.Values["Heartrate"][2];

                Assert.Equal(baseline.AddMinutes(40), range.From);
                Assert.Equal(baseline.AddMinutes(60), range.To);

                Assert.Equal(121, range.Values.Length);
                Assert.Equal(baseline.AddMinutes(40), range.Values[0].Timestamp);
                Assert.Equal(baseline.AddMinutes(60), range.Values[120].Timestamp);
            }
        }
    }
}
