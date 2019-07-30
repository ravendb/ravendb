using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Replication
{
    public class TimeSeriesReplicationTests : ReplicationTestBase
    {

        [Fact]
        public async Task CanReplicate1()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = storeA.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                using (var session = storeB.OpenSession())
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
        public async Task CanReplicate2()
        {
            using (var src = GetDocumentStore())
            using (var dst = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = src.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                var offset = 0;

                for (int i = 0; i < 10; i++)
                {
                    using (var session = src.OpenSession())
                    {

                        for (int j = 0; j < 1000; j++)
                        {
                            session.TimeSeriesFor("users/ayende")
                                .Append("Heartrate", baseline.AddMinutes(offset++), "watches/fitbit", new double[] { offset });
                        }

                        session.SaveChanges();
                    }
                }

                await SetupReplicationAsync(src, dst);
                EnsureReplicating(src, dst);

                using (var session = dst.OpenSession())
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
    }
}
