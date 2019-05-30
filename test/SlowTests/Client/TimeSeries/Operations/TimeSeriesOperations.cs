using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Exceptions.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.TimeSeries.Operations
{
    public class TimeSeriesOperations : RavenTestBase
    {

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
                Assert.Equal(1, timesSeriesDetails.Values["Heartrate"].Values.Length);

                var value = timesSeriesDetails.Values["Heartrate"].Values[0];

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
                Assert.Equal(3, timesSeriesDetails.Values["Heartrate"].Values.Length);

                var value = timesSeriesDetails.Values["Heartrate"].Values[0];

                Assert.Equal(59d, value.Values[0]);
                Assert.Equal("watches/fitbit", value.Tag);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp);

                value = timesSeriesDetails.Values["Heartrate"].Values[1];

                Assert.Equal(61d, value.Values[0]);
                Assert.Equal("watches/fitbit", value.Tag);
                Assert.Equal(baseline.AddSeconds(2), value.Timestamp);


                value = timesSeriesDetails.Values["Heartrate"].Values[2];

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
                Assert.Equal(5, timesSeriesDetails.Values["Heartrate"].Values.Length);


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
                Assert.Equal(3, timesSeriesDetails.Values["Heartrate"].Values.Length);

                var value = timesSeriesDetails.Values["Heartrate"].Values[0];
                Assert.Equal(59d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp);

                value = timesSeriesDetails.Values["Heartrate"].Values[1];
                Assert.Equal(62.5d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(4), value.Timestamp);

                value = timesSeriesDetails.Values["Heartrate"].Values[2];
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
                Assert.Equal(3, timesSeriesDetails.Values["Heartrate"].Values.Length);

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
                Assert.Equal(4, timesSeriesDetails.Values["Heartrate"].Values.Length);

                var value = timesSeriesDetails.Values["Heartrate"].Values[0];
                Assert.Equal(59d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp);

                value = timesSeriesDetails.Values["Heartrate"].Values[1];
                Assert.Equal(60d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(4), value.Timestamp);

                value = timesSeriesDetails.Values["Heartrate"].Values[2];
                Assert.Equal(62.5d, value.Values[0]);
                Assert.Equal(baseline.AddSeconds(5), value.Timestamp);

                value = timesSeriesDetails.Values["Heartrate"].Values[3];
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


    }
}
