using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14994 : RavenTestBase
    {
        public RavenDB_14994(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void GetOnNonExistingTimeSeriesShouldReturnNull()
        {
            using (var store = GetDocumentStore())
            {
                var documentId = "users/ayende";

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var get = store.Operations.Send(new GetTimeSeriesOperation(documentId, "HeartRate"));
                Assert.Null(get);

                using (var session = store.OpenSession())
                {
                    var entries = session.TimeSeriesFor(documentId, "HeartRate").Get()?.ToList();
                    Assert.Null(entries);
                }
            }
        }

        [Fact]
        public void GetOnEmptyRangeShouldReturnEmptyArray()
        {
            using (var store = GetDocumentStore())
            {
                var documentId = "users/ayende";
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);

                    var tsf = session.TimeSeriesFor(documentId, "HeartRate");

                    for (int i = 0; i < 10; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i);
                    }

                    session.SaveChanges();
                }

                var get = store.Operations.Send(new GetTimeSeriesOperation(documentId, "HeartRate", baseline.AddMonths(-2), baseline.AddMonths(-1)));
                Assert.Empty(get.Entries);

                using (var session = store.OpenSession())
                {
                    var entries = session.TimeSeriesFor(documentId, "HeartRate").Get(baseline.AddMonths(-2), baseline.AddMonths(-1))?.ToList();
                    Assert.Empty(entries);
                }
            }
        }

        [Fact]
        public void ConstantTimeValuesShouldReturnConstantHashCodes()
        {
            var zero = TimeValue.Zero;
            var anotherZero = TimeValue.FromHours(5) - TimeValue.FromHours(5);

            var h1 = zero.GetHashCode();
            var h2 = anotherZero.GetHashCode();
            Assert.Equal(h2, h1);
            Assert.Equal(zero, anotherZero);
        }
    }
}
