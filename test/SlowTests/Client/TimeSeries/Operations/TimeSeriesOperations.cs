using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions.Documents;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Client.TimeSeries.Query;
using Sparrow;
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
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var baseline = RavenTestHelper.UtcToday;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                };
                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/fitbit", Timestamp = baseline.AddSeconds(1), Values = new[] {59d}});
                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var timeSeriesRangeResult = store.Operations.Send(
                    new GetTimeSeriesOperation(documentId, "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal(1, timeSeriesRangeResult.Entries.Length);

                var value = timeSeriesRangeResult.Entries[0];

                Assert.Equal(59d, value.Values[0]);
                Assert.Equal("watches/fitbit", value.Tag);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
            }
        }

        [Fact]
        public void CanGetNonExistedRange()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.SaveChanges();
                }

                var baseline = RavenTestHelper.UtcToday;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                };
                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/fitbit", Timestamp = baseline.AddSeconds(1), Values = new[] {59d}});

                var timeSeriesBatch = new TimeSeriesBatchOperation("users/ayende", timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var timeSeriesRangeResult = store.Operations.Send(
                    new GetTimeSeriesOperation("users/ayende", "Heartrate", baseline.AddMonths(-2), baseline.AddMonths(-1)));

                Assert.Empty(timeSeriesRangeResult.Entries);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>().Select(u => RavenQuery.TimeSeries(u, "Heartrate", baseline.AddMonths(-2), baseline.AddMonths(-1)).ToList()).ToList();
                    Assert.Equal(0, query[0].Results.Length);
                }
            }
        }

        [Fact]
        public void CanStoreAndReadMultipleTimestampsUsingStoreOperations()
        {
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var baseline = RavenTestHelper.UtcToday;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                };

                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/fitbit", Timestamp = baseline.AddSeconds(1), Values = new[] {59d}});
                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/fitbit", Timestamp = baseline.AddSeconds(2), Values = new[] {61d}});
                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/apple-watch", Timestamp = baseline.AddSeconds(5), Values = new[] {60d}});


                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var timeSeriesRangeResult = store.Operations.Send(
                    new GetTimeSeriesOperation(documentId, "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal(3, timeSeriesRangeResult.Entries.Length);

                var value = timeSeriesRangeResult.Entries[0];

                Assert.Equal(59d, value.Values[0]);
                Assert.Equal("watches/fitbit", value.Tag);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                value = timeSeriesRangeResult.Entries[1];

                Assert.Equal(61d, value.Values[0]);
                Assert.Equal("watches/fitbit", value.Tag);
                Assert.Equal(baseline.AddSeconds(2), value.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                value = timeSeriesRangeResult.Entries[2];

                Assert.Equal(60d, value.Values[0]);
                Assert.Equal("watches/apple-watch", value.Tag);
                Assert.Equal(baseline.AddSeconds(5), value.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
            }
        }

        [Fact]
        public void CanDeleteTimestampUsingStoreOperations()
        {
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var baseline = RavenTestHelper.UtcToday;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                };

                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/fitbit", Timestamp = baseline.AddSeconds(1), Values = new[] {59d}});
                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/fitbit", Timestamp = baseline.AddSeconds(2), Values = new[] {61d}});
                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/fitbit", Timestamp = baseline.AddSeconds(3), Values = new[] {60d}});
                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/fitbit", Timestamp = baseline.AddSeconds(4), Values = new[] {62.5d}});
                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/fitbit", Timestamp = baseline.AddSeconds(5), Values = new[] {62d}});


                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var timeSeriesRangeResult = store.Operations.Send(
                    new GetTimeSeriesOperation(documentId, "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal(5, timeSeriesRangeResult.Entries.Length);


                timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                };
                timeSeriesOp.Delete(new TimeSeriesOperation.DeleteOperation {From = baseline.AddSeconds(2), To = baseline.AddSeconds(3)});
                timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                timeSeriesRangeResult = store.Operations.Send(
                    new GetTimeSeriesOperation(documentId, "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal(3, timeSeriesRangeResult.Entries.Length);

                var value = timeSeriesRangeResult.Entries[0];
                Assert.Equal(59d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                value = timeSeriesRangeResult.Entries[1];
                Assert.Equal(62.5d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(4), value.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                value = timeSeriesRangeResult.Entries[2];
                Assert.Equal(62d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(5), value.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

            }


            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, documentId);
                    var tsf = session.TimeSeriesFor(documentId, "Heartrate");
                    tsf.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 69d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(3), new[] { 79d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, documentId);
                    session.TimeSeriesFor(documentId, "Heartrate")
                        .Delete(baseline.AddMinutes(2));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId, "Heartrate")
                         .Get(DateTime.MinValue, DateTime.MaxValue)
                         .ToList();
                    Assert.Equal(2, vals.Count);
                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 79d }, vals[1].Values);
                    Assert.Equal("watches/fitbit", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanDeleteLargeRange()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.AddSeconds(-1);

                using (var session = store.OpenSession())
                {

                    session.Store(new User(), "foo/bar");
                    var tsf = session.TimeSeriesFor("foo/bar", "BloodPressure");

                    for (int j = 1; j < 10_000; j++)
                    {
                        var offset = j * 10;
                        var time = baseline.AddSeconds(offset);

                        tsf.Append(time, new[] { (double)(j) }, "watches/apple");
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
                    var query = session.Advanced.RawQuery<TimeSeriesRawQuery.RawQueryResult>(rawQuery)
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);

                    var agg = result[0];

                    var bloodPressure = agg.BloodPressure;
                    var count = bloodPressure.Results.Sum(r => r.Count[0]);
                    Assert.Equal(8640, count);
                    Assert.Equal(bloodPressure.Count, count);
                    Assert.Equal(24, bloodPressure.Results.Length);

                    for (var index = 0; index < bloodPressure.Results.Length; index++)
                    {
                        var item = bloodPressure.Results[index];
                        Assert.Equal(360, item.Count[0]);
                        Assert.Equal(index * 360 + 180 + 0.5, item.Average[0]);
                        Assert.Equal((index + 1) * 360, item.Max[0]);
                        Assert.Equal(index * 360 + 1, item.Min[0]);
                        Assert.Equal(index * 360 + 1, item.First[0]);
                        Assert.Equal((index + 1) * 360, item.Last[0]);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("foo/bar", "BloodPressure");
                    tsf.Delete(baseline.AddSeconds(3600), baseline.AddSeconds(3600 * 10)); // remove 9 hours
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoCaching = true
                }))
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawQuery.RawQueryResult>(rawQuery)
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

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
                    Assert.Equal(180, item.Average[0]);
                    Assert.Equal(359, item.Max[0]);
                    Assert.Equal(1, item.Min[0]);
                    Assert.Equal(1, item.First[0]);
                    Assert.Equal(359, item.Last[0]);

                    for (index = 1; index < bloodPressure.Results.Length; index++)
                    {
                        item = bloodPressure.Results[index];
                        var realIndex = index + 9;

                        Assert.Equal(360, item.Count[0]);
                        Assert.Equal(realIndex * 360 + 180 + 0.5, item.Average[0]);
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
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var baseline = RavenTestHelper.UtcToday;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                };

                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/fitbit", Timestamp = baseline.AddSeconds(1), Values = new[] {59d}});
                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/fitbit", Timestamp = baseline.AddSeconds(2), Values = new[] {61d}});
                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/fitbit", Timestamp = baseline.AddSeconds(3), Values = new[] {61.5d}});


                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var timeSeriesRangeResult = store.Operations.Send(
                    new GetTimeSeriesOperation(documentId, "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal(3, timeSeriesRangeResult.Entries.Length);

                timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                };

                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/fitbit", Timestamp = baseline.AddSeconds(4), Values = new[] {60d}});
                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/fitbit", Timestamp = baseline.AddSeconds(5), Values = new[] {62.5d}});
                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/fitbit", Timestamp = baseline.AddSeconds(6), Values = new[] {62d}});
                timeSeriesOp.Delete(new TimeSeriesOperation.DeleteOperation {From = baseline.AddSeconds(2), To = baseline.AddSeconds(3)});

                timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                timeSeriesRangeResult = store.Operations.Send(
                    new GetTimeSeriesOperation(documentId, "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal(4, timeSeriesRangeResult.Entries.Length);

                var value = timeSeriesRangeResult.Entries[0];
                Assert.Equal(59d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                value = timeSeriesRangeResult.Entries[1];
                Assert.Equal(60d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(4), value.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                value = timeSeriesRangeResult.Entries[2];
                Assert.Equal(62.5d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(5), value.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                value = timeSeriesRangeResult.Entries[3];
                Assert.Equal(62d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(6), value.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

            }

            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, documentId);
                    session.TimeSeriesFor(documentId, "Heartrate")
                        .Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    session.TimeSeriesFor(documentId, "Heartrate")
                        .Append(baseline.AddMinutes(2), new[] { 69d }, "watches/fitbit");
                    session.TimeSeriesFor(documentId, "Heartrate")
                     .Append(baseline.AddMinutes(3), new[] { 79d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, documentId);
                    session.TimeSeriesFor(documentId, "Heartrate")
                        .Delete(baseline.AddMinutes(2));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId, "Heartrate")
                         .Get(DateTime.MinValue, DateTime.MaxValue)
                         .ToList();
                    Assert.Equal(2, vals.Count);
                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 79d }, vals[1].Values);
                    Assert.Equal("watches/fitbit", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void ShouldThrowOnAttemptToCreateTimeSeriesOnMissingDocument()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                };
                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Tag = "watches/fitbit", Timestamp = baseline.AddSeconds(1), Values = new[] {59d}});

                var timeSeriesBatch = new TimeSeriesBatchOperation("users/ayende", timeSeriesOp);

                var ex = Assert.Throws<DocumentDoesNotExistException>(() => store.Operations.Send(timeSeriesBatch));

                Assert.Contains("Cannot operate on time series of a missing document", ex.Message);
            }
        }

        [Fact]
        public void CanGetMultipleRangesInSingleRequest()
        {
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var baseline = RavenTestHelper.UtcToday;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                };

                for (int i = 0; i <= 360; i++)
                {
                    timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation
                    {
                        Tag = "watches/fitbit",
                        Timestamp = baseline.AddSeconds(i * 10),
                        Values = new[] { 59d }
                    });
                }

                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var timesSeriesDetails = store.Operations.Send(
                    new GetMultipleTimeSeriesOperation(documentId, new List<TimeSeriesRange>
                    {
                        new TimeSeriesRange
                        {
                            Name = "Heartrate",
                            From = baseline.AddMinutes(5),
                            To = baseline.AddMinutes(10)
                        },

                        new TimeSeriesRange
                        {
                            Name = "Heartrate",
                            From = baseline.AddMinutes(15),
                            To = baseline.AddMinutes(30)
                        },

                        new TimeSeriesRange
                        {
                            Name = "Heartrate",
                            From = baseline.AddMinutes(40),
                            To = baseline.AddMinutes(60)
                        }
                    }));

                Assert.Equal(documentId, timesSeriesDetails.Id);
                Assert.Equal(1, timesSeriesDetails.Values.Count);
                Assert.Equal(3, timesSeriesDetails.Values["Heartrate"].Count);

                var range = timesSeriesDetails.Values["Heartrate"][0];

                Assert.Equal(baseline.AddMinutes(5), range.From, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(10), range.To, RavenTestHelper.DateTimeComparer.Instance);

                Assert.Equal(31, range.Entries.Length);
                Assert.Equal(baseline.AddMinutes(5), range.Entries[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(10), range.Entries[30].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                range = timesSeriesDetails.Values["Heartrate"][1];

                Assert.Equal(baseline.AddMinutes(15), range.From, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(30), range.To, RavenTestHelper.DateTimeComparer.Instance);

                Assert.Equal(91, range.Entries.Length);
                Assert.Equal(baseline.AddMinutes(15), range.Entries[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(30), range.Entries[90].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                range = timesSeriesDetails.Values["Heartrate"][2];

                Assert.Equal(baseline.AddMinutes(40), range.From, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(60), range.To, RavenTestHelper.DateTimeComparer.Instance);

                Assert.Equal(121, range.Entries.Length);
                Assert.Equal(baseline.AddMinutes(40), range.Entries[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(60), range.Entries[120].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
            }
        }

        [Fact]
        public void CanGetMultipleTimeSeriesInSingleRequest()
        {
            using (var store = GetDocumentStore())
            {
                var documentId = "users/ayende";
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                // append

                var baseline = RavenTestHelper.UtcToday;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                };

                for (int i = 0; i <= 10; i++)
                {
                    timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation
                    {
                        Tag = "watches/fitbit",
                        Timestamp = baseline.AddMinutes(i * 10),
                        Values = new[] { 72d }
                    });
                }

                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "BloodPressure",
                };

                for (int i = 0; i <= 10; i++)
                {
                    timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation
                    {
                        Timestamp = baseline.AddMinutes(i * 10),
                        Values = new[] { 80d }
                    });
                }

                timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Temperature",
                };

                for (int i = 0; i <= 10; i++)
                {
                    timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation
                    {
                        Timestamp = baseline.AddMinutes(i * 10),
                        Values = new[] { 37d + i * 0.15 }
                    });
                }

                timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                // get ranges from multiple time series in a single request

                var timesSeriesDetails = store.Operations.Send(
                    new GetMultipleTimeSeriesOperation(documentId, new List<TimeSeriesRange>
                    {
                        new TimeSeriesRange
                        {
                            Name = "Heartrate",
                            From = baseline,
                            To = baseline.AddMinutes(15)
                        },
                        new TimeSeriesRange
                        {
                            Name = "Heartrate",
                            From = baseline.AddMinutes(30),
                            To = baseline.AddMinutes(45)
                        },
                        new TimeSeriesRange
                        {
                            Name = "BloodPressure",
                            From = baseline,
                            To = baseline.AddMinutes(30)
                        },
                        new TimeSeriesRange
                        {
                            Name = "BloodPressure",
                            From = baseline.AddMinutes(60),
                            To = baseline.AddMinutes(90)
                        },
                        new TimeSeriesRange
                        {
                            Name = "Temperature",
                            From = baseline,
                            To = baseline.AddDays(1)
                        }
                    }));

                Assert.Equal("users/ayende", timesSeriesDetails.Id);
                Assert.Equal(3, timesSeriesDetails.Values.Count);

                Assert.Equal(2, timesSeriesDetails.Values["Heartrate"].Count);

                var range = timesSeriesDetails.Values["Heartrate"][0];

                Assert.Equal(baseline, range.From, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(15), range.To, RavenTestHelper.DateTimeComparer.Instance);

                Assert.Equal(2, range.Entries.Length);
                Assert.Equal(baseline, range.Entries[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(10), range.Entries[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                Assert.Null(range.TotalResults);

                range = timesSeriesDetails.Values["Heartrate"][1];

                Assert.Equal(baseline.AddMinutes(30), range.From, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(45), range.To, RavenTestHelper.DateTimeComparer.Instance);

                Assert.Equal(2, range.Entries.Length);
                Assert.Equal(baseline.AddMinutes(30), range.Entries[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(40), range.Entries[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                Assert.Null(range.TotalResults);

                Assert.Equal(2, timesSeriesDetails.Values["BloodPressure"].Count);

                range = timesSeriesDetails.Values["BloodPressure"][0];

                Assert.Equal(baseline, range.From, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(30), range.To, RavenTestHelper.DateTimeComparer.Instance);

                Assert.Equal(4, range.Entries.Length);
                Assert.Equal(baseline, range.Entries[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(30), range.Entries[3].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                Assert.Null(range.TotalResults);

                range = timesSeriesDetails.Values["BloodPressure"][1];

                Assert.Equal(baseline.AddMinutes(60), range.From, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(90), range.To, RavenTestHelper.DateTimeComparer.Instance);

                Assert.Equal(4, range.Entries.Length);
                Assert.Equal(baseline.AddMinutes(60), range.Entries[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(90), range.Entries[3].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                Assert.Null(range.TotalResults);

                Assert.Equal(1, timesSeriesDetails.Values["Temperature"].Count);

                range = timesSeriesDetails.Values["Temperature"][0];

                Assert.Equal(baseline, range.From, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddDays(1), range.To, RavenTestHelper.DateTimeComparer.Instance);

                Assert.Equal(11, range.Entries.Length);
                Assert.Equal(baseline, range.Entries[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(100), range.Entries[10].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                Assert.Equal(11, range.TotalResults); // full range
            }
        }

        [Fact]
        public void ShouldThrowOnNullOrEmptyRanges()
        {
            using (var store = GetDocumentStore())
            {
                var documentId = "users/ayende";
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var baseline = RavenTestHelper.UtcToday;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                };

                for (int i = 0; i <= 10; i++)
                {
                    timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation
                    {
                        Tag = "watches/fitbit",
                        Timestamp = baseline.AddMinutes(i * 10),
                        Values = new[] { 72d }
                    });
                }

                var timeSeriesBatch = new TimeSeriesBatchOperation("users/ayende", timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);
                var ex = Assert.Throws<ArgumentNullException>(() => store.Operations.Send(
                    new GetMultipleTimeSeriesOperation("users/ayende", null)));

                Assert.Contains("Value cannot be null. (Parameter 'ranges')", ex.Message);

                var ex2 = Assert.Throws<InvalidOperationException>(() => store.Operations.Send(
                    new GetMultipleTimeSeriesOperation("users/ayende", new List<TimeSeriesRange>())));

                Assert.Contains("'ranges' cannot be null or empty", ex2.Message);
            }
        }

        [Fact]
        public void GetMultipleTimeSeriesShouldThrowOnMissingNameFromRange()
        {
            using (var store = GetDocumentStore())
            {
                var documentId = "users/ayende";

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var baseline = RavenTestHelper.UtcToday;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                };

                for (int i = 0; i <= 10; i++)
                {
                    timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation
                    {
                        Tag = "watches/fitbit",
                        Timestamp = baseline.AddMinutes(i * 10),
                        Values = new[] { 72d }
                    });
                }

                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var ex = Assert.Throws<InvalidOperationException>(() => store.Operations.Send(
                    new GetMultipleTimeSeriesOperation("users/ayende", new List<TimeSeriesRange>
                    {
                        new TimeSeriesRange
                        {
                            From = baseline,
                            To = DateTime.MaxValue
                        }
                    })));

                Assert.Contains("'Name' cannot be null or empty", ex.Message);
            }
        }

        [Fact]
        public void GetTimeSeriesShouldThrowOnMissingName()
        {
            using (var store = GetDocumentStore())
            {
                var documentId = "users/ayende";

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var baseline = RavenTestHelper.UtcToday;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                };

                for (int i = 0; i <= 10; i++)
                {
                    timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation
                    {
                        Tag = "watches/fitbit",
                        Timestamp = baseline.AddMinutes(i * 10),
                        Values = new[] { 72d }
                    });
                }

                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var ex = Assert.Throws<ArgumentNullException>(() => store.Operations.Send(
                    new GetTimeSeriesOperation("users/ayende", string.Empty, baseline, DateTime.MaxValue)));

                Assert.Contains("Value cannot be null", ex.Message);
            }
        }

        [Fact]
        public async Task GetTimeSeriesStatistics()
        {
            using (var store = GetDocumentStore())
            {
                var documentId = "users/ayende";
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    var ts = session.TimeSeriesFor(documentId, "heartrate");
                    for (int i = 0; i <= 10; i++)
                    {
                        ts.Append(baseline.AddMinutes(i * 10), 72d, "watches/fitbit");
                    }
                    
                    ts = session.TimeSeriesFor(documentId, "pressure");
                    for (int i = 10; i <= 20; i++)
                    {
                        ts.Append(baseline.AddMinutes(i * 10), 72d, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                var op = await store.Operations.SendAsync(new GetTimeSeriesStatisticsOperation(documentId));
                Assert.Equal(documentId, op.DocumentId);
                Assert.Equal(2, op.TimeSeries.Count);
                var ts1 = op.TimeSeries[0];
                var ts2 = op.TimeSeries[1];

                Assert.Equal("heartrate", ts1.Name);
                Assert.Equal("pressure", ts2.Name);

                Assert.Equal(11, ts1.NumberOfEntries);
                Assert.Equal(11, ts2.NumberOfEntries);

                Assert.Equal(baseline, ts1.StartDate, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(10 * 10), ts1.EndDate, RavenTestHelper.DateTimeComparer.Instance);

                Assert.Equal(baseline.AddMinutes(10 * 10), ts2.StartDate, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(20 * 10), ts2.EndDate, RavenTestHelper.DateTimeComparer.Instance);

            }
        }

        [Fact]
        public void CanDeleteWithoutProvidingFromAndToDates()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                var docId = "users/ayende";

                using (var session = store.OpenSession())
                {

                    session.Store(new User(), docId);

                    var tsf = session.TimeSeriesFor(docId, "HeartRate");
                    var tsf2 = session.TimeSeriesFor(docId, "BloodPressure");
                    var tsf3 = session.TimeSeriesFor(docId, "BodyTemperature");

                    for (int j = 0; j < 100; j++)
                    {
                        tsf.Append(baseline.AddMinutes(j), j);
                        tsf2.Append(baseline.AddMinutes(j), j);
                        tsf3.Append(baseline.AddMinutes(j), j);
                    }

                    session.SaveChanges();
                }

                var get = store.Operations.Send(new GetTimeSeriesOperation(docId, "HeartRate"));
                Assert.Equal(100, get.Entries.Length);

                // null From, To
                var deleteOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                };

                deleteOp.Delete(new TimeSeriesOperation.DeleteOperation());

                store.Operations.Send(new TimeSeriesBatchOperation(docId, deleteOp));

                get = store.Operations.Send(new GetTimeSeriesOperation(docId, "HeartRate"));
                Assert.Null(get);

                get = store.Operations.Send(new GetTimeSeriesOperation(docId, "BloodPressure"));
                Assert.Equal(100, get.Entries.Length);

                // null To
                deleteOp = new TimeSeriesOperation
                {
                    Name = "BloodPressure",
                };
                deleteOp.Delete(new TimeSeriesOperation.DeleteOperation {From = baseline.AddMinutes(50), To = null});

                store.Operations.Send(new TimeSeriesBatchOperation(docId, deleteOp));

                get = store.Operations.Send(new GetTimeSeriesOperation(docId, "BloodPressure"));
                Assert.Equal(50, get.Entries.Length);

                get = store.Operations.Send(new GetTimeSeriesOperation(docId, "BodyTemperature"));
                Assert.Equal(100, get.Entries.Length);

                // null From
                deleteOp = new TimeSeriesOperation
                {
                    Name = "BodyTemperature",
                };
                deleteOp.Delete(new TimeSeriesOperation.DeleteOperation {To = baseline.AddMinutes(19)});

                store.Operations.Send(new TimeSeriesBatchOperation(docId, deleteOp));

                get = store.Operations.Send(new GetTimeSeriesOperation(docId, "BodyTemperature"));
                Assert.Equal(80, get.Entries.Length);

            }
        }
        
        [Theory]
        [InlineData(0)]
        [InlineData(12)]
        [InlineData(123)]
        [InlineData(456)]
        [InlineData(720)]
        [InlineData(789)]
        [InlineData(1421)]
        public async Task ValidateCorrectRetentionAndGet(int minutesOffset)
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeSpan.FromHours(24));

                var p1 = new TimeSeriesPolicy("By6Hours", TimeSpan.FromHours(6), raw.RetentionTime * 4);
                var p2 = new TimeSeriesPolicy("By1Day", TimeSpan.FromDays(1), raw.RetentionTime * 5);
                var p3 = new TimeSeriesPolicy("By30Minutes", TimeSpan.FromMinutes(30), raw.RetentionTime * 2);
                var p4 = new TimeSeriesPolicy("By1Hour", TimeSpan.FromMinutes(60), raw.RetentionTime * 3);

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3,p4
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = new DateTime(2020, 4, 2).AddMinutes(minutesOffset);
                database.Time.UtcDateTime = () => now.AddMilliseconds(1);

                var baseline = now.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Core.Utils.Entities.User { Name = "Karmel" }, "users/karmel");
                    for (int i = 0; i <= total; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await QueryFromMultipleTimeSeries.VerifyFullPolicyExecution(store, config.Collections["Users"]);
            }
        }
    }
}
