using System;
using FastTests;
using Xunit;
using Xunit.Abstractions;
using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_16906 : RavenTestBase
    {
        public RavenDB_16906(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TimeSeriesFor_ShouldThrowBetterError_OnNullEntity()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var e = Assert.Throws<ArgumentNullException>(() => session.TimeSeriesFor(user, "HeartRate"));
                    Assert.Contains("Value cannot be null. (Parameter 'entity')", e.Message);
                }
            }
        }
    }
}
