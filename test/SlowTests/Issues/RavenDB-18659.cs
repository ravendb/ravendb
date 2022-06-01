using System;
using FastTests;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18659 : RavenTestBase
    {
        public RavenDB_18659(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanStreamTimeSeriesWithFromAndTo()
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
                        ts.Append(baseline.AddMinutes(i), i, "stream");
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("karmel", "heartrate");

                    using (var it = ts.Stream(baseline.AddMinutes(1), baseline.AddMinutes(9)))
                    {
                        var i = 1;
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
    }
}
