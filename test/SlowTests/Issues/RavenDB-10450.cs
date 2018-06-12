using System;
using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10450 : RavenTestBase
    {
        [Fact]
        public async Task Slow_IO_hints_are_stored_and_can_be_read()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                database.NotificationCenter
                                 .SlowWrites
                    .Add("C:\\Raven", 1, 10);

                database.NotificationCenter
                    .SlowWrites
                    .Add("C:\\Raven\\Indexes\\1", 1, 20);

                database.NotificationCenter
                    .SlowWrites
                    .Add("C:\\Raven", 2, 10);

                database.NotificationCenter.SlowWrites.UpdateNotificationInStorage(null);

                var details = database.NotificationCenter.SlowWrites
                    .GetSlowWritesDetails();

                Assert.Equal(2, details.Writes.Count);
                Assert.Equal(1, details.Writes["C:\\Raven"].DataWrittenInMb);
                Assert.Equal(10, details.Writes["C:\\Raven"].DurationInSec);

                Assert.Equal(1, details.Writes["C:\\Raven\\Indexes\\1"].DataWrittenInMb);
                Assert.Equal(20, details.Writes["C:\\Raven\\Indexes\\1"].DurationInSec);

                database.NotificationCenter.SlowWrites.UpdateFrequency = TimeSpan.Zero;

                database.NotificationCenter
                    .SlowWrites
                    .Add("C:\\Raven", 2, 30);

                database.NotificationCenter.SlowWrites.UpdateNotificationInStorage(null);

                details = database.NotificationCenter.SlowWrites
                    .GetSlowWritesDetails();

                Assert.Equal(2, details.Writes.Count);
                Assert.Equal(2, details.Writes["C:\\Raven"].DataWrittenInMb);
                Assert.Equal(30, details.Writes["C:\\Raven"].DurationInSec);
            }
        }
    }
}
