using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Server;
using Sparrow.Server.Meters;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12040 : RavenTestBase
    {
        public RavenDB_12040(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Should_limit_number_of_stored_slow_io_hints()
        {
            var now = DateTime.UtcNow;

            using (var store = GetDocumentStore())
            {
                const int limitExceededCount = 10;

                var database = await GetDatabase(store.Database);

                for (int i = 0; i < SlowIoDetails.MaxNumberOfWrites + limitExceededCount; i++)
                {
                    database.NotificationCenter
                        .SlowWrites
                        .Add(new IoChange
                        {
                            FileName = $"C:\\Raven\\Indexes\\MyIndex\\{i}.journal",
                            MeterItem = new IoMeterBuffer.MeterItem 
                            { 
                                Type = IoMetrics.MeterType.JournalWrite,
                                Start = now, 
                                End = now + TimeSpan.FromSeconds(10)
                            }
                        });

                    if (i < limitExceededCount)
                        Thread.Sleep(1);
                }

                database.NotificationCenter.SlowWrites.UpdateNotificationInStorage(null);

                var details = database.NotificationCenter.SlowWrites
                    .GetSlowIoDetails();

                Assert.Equal(SlowIoDetails.MaxNumberOfWrites, details.Writes.Count);

                for (int i = limitExceededCount; i < SlowIoDetails.MaxNumberOfWrites + limitExceededCount; i++)
                {
                    Assert.Contains($"{nameof(IoMetrics.MeterType.JournalWrite)}/C:\\Raven\\Indexes\\MyIndex\\{i}.journal", details.Writes.Keys);
                }
            }
        }
    }
}
