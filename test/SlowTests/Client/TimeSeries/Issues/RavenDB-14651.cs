using System;
using System.Collections.Generic;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14651 : ReplicationTestBase
    {
        public RavenDB_14651(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanAvoidPassingTagInBatchOperation()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.SaveChanges();
                }

                var baseline = DateTime.Today;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    DocumentId = "users/ayende",
                    Appends = new List<TimeSeriesOperation.AppendOperation>()
                    {
                        new TimeSeriesOperation.AppendOperation
                        {
                            Name = "Heartrate",
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
                Assert.Equal(1, timesSeriesDetails.Values["Heartrate"][0].Entries.Length);

                var value = timesSeriesDetails.Values["Heartrate"][0].Entries[0];

                Assert.Equal(59d, value.Values[0]);
                Assert.Null(value.Tag);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp);

            }
        }


        [Fact]
        public void CanAvoidPassingTagInSessionAppend()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.SaveChanges();
                }

                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddSeconds(1), new []{ 59d });
                    session.SaveChanges();
                }


                var timesSeriesDetails = store.Operations.Send(
                    new GetTimeSeriesOperation("users/ayende", "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal("users/ayende", timesSeriesDetails.Id);
                Assert.Equal(1, timesSeriesDetails.Values.Count);
                Assert.Equal(1, timesSeriesDetails.Values["Heartrate"][0].Entries.Length);

                var value = timesSeriesDetails.Values["Heartrate"][0].Entries[0];

                Assert.Equal(59d, value.Values[0]);
                Assert.Null(value.Tag);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp);

            }
        }

        [Fact]
        public void CanUseSingleValueInSessionAppend()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.SaveChanges();
                }

                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddSeconds(1), 59, "watches/fitbit");
                    session.SaveChanges();
                }

                var timesSeriesDetails = store.Operations.Send(
                    new GetTimeSeriesOperation("users/ayende", "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal("users/ayende", timesSeriesDetails.Id);
                Assert.Equal(1, timesSeriesDetails.Values.Count);
                Assert.Equal(1, timesSeriesDetails.Values["Heartrate"][0].Entries.Length);

                var value = timesSeriesDetails.Values["Heartrate"][0].Entries[0];

                Assert.Equal(59d, value.Values[0]);
                Assert.Equal("watches/fitbit", value.Tag);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp);

            }
        }

    }
}
