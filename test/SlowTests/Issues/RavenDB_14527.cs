using System;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14527 : RavenTestBase
    {
        [Fact]
        public void CanHandleLargeTimeDelta()
        {
            using var store = GetDocumentStore();
            using var session = store.OpenSession();
            session.Store(new {}, "nodes/2-A");
            var timeSeries = session.TimeSeriesFor("nodes/2-A");

            var start = DateTime.Today.AddDays(-3);
            var random = new Random(1337);
            var n1 = 0.4;
            var n2 = 0.27;
            var n1t = 0;
            var n2t = 2;
            for (int i = 0; i < 350; i++)
            {
                var up = random.Next(10) > 5;
                for (int j = 0; j < random.Next(7, 18); j++)
                {
                    n1 += random.NextDouble() * (up ? 1 : -1);
                    if (n1 < 0)
                    {
                        n1 = 0;
                        up = true;
                    }
                    timeSeries.Append("DiskQueue",
                        start.AddSeconds(n1t).AddMilliseconds(150),
                        "/dev/nvme1",
                        new[] { n1 }
                    );

                    n1t += random.Next(10);
                }

                for (int j = 0; j < random.Next(7, 18); j++)
                {
                    n2 += random.NextDouble() * (up ? 1 : -1);
                    if (n2 < 0)
                    {
                        n2 = 0;
                        up = true;
                    }
                    timeSeries.Append("DiskQueue",
                        start.AddSeconds(n1t).AddMilliseconds(250),
                        "/dev/nvme2",
                        new[] { n2 }
                    );

                    n2t += random.Next(10);
                }
            }
            session.SaveChanges();
        }

        public RavenDB_14527(ITestOutputHelper output) : base(output)
        {
        }
    }
}
