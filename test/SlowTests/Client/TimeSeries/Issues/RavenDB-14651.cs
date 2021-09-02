using System;
using FastTests;
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
                timeSeriesOp.Append(new TimeSeriesOperation.AppendOperation {Timestamp = baseline.AddSeconds(1), Values = new[] {59d}});
                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var rangeResult = store.Operations.Send(
                    new GetTimeSeriesOperation("users/ayende", "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal(1, rangeResult.Entries.Length);

                var value = rangeResult.Entries[0];

                Assert.Equal(59d, value.Values[0]);
                Assert.Null(value.Tag);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

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

                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddSeconds(1), new []{ 59d });
                    session.SaveChanges();
                }

                var rangeResult = store.Operations.Send(
                    new GetTimeSeriesOperation("users/ayende", "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal(1, rangeResult.Entries.Length);

                var value = rangeResult.Entries[0];

                Assert.Equal(59d, value.Values[0]);
                Assert.Null(value.Tag);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
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

                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddSeconds(1), 59, "watches/fitbit");
                    session.SaveChanges();
                }

                var rangeResult = store.Operations.Send(
                    new GetTimeSeriesOperation("users/ayende", "Heartrate", DateTime.MinValue, DateTime.MaxValue));

                Assert.Equal(1, rangeResult.Entries.Length);

                var value = rangeResult.Entries[0];

                Assert.Equal(59d, value.Values[0]);
                Assert.Equal("watches/fitbit", value.Tag);
                Assert.Equal(baseline.AddSeconds(1), value.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
            }
        }
    }
}
