using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.NotificationCenter.Notifications.Details;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12040 : RavenTestBase
    {
        [Fact]
        public async Task Should_limit_number_of_stored_slow_io_hints()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                for (int i = 0; i < SlowWritesDetails.MaxNumberOfWrites + 10; i++)
                {
                    database.NotificationCenter
                        .SlowWrites
                        .Add($"C:\\Raven\\Indexes\\MyIndex\\{i}.journal", 1, 10);

                    Thread.Sleep(1);
                }

                database.NotificationCenter.SlowWrites.UpdateNotificationInStorage(null);

                var details = database.NotificationCenter.SlowWrites
                    .GetSlowWritesDetails();

                Assert.Equal(SlowWritesDetails.MaxNumberOfWrites, details.Writes.Count);

                for (int i = 10; i < SlowWritesDetails.MaxNumberOfWrites + 10; i++)
                {
                    Assert.Contains($"C:\\Raven\\Indexes\\MyIndex\\{i}.journal", details.Writes.Keys);
                }
            }
        }
    }
}
