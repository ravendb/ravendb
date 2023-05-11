using System;
using System.Threading.Tasks;
using FastTests;
using Sparrow.Server;
using Sparrow.Server.Meters;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10450 : RavenTestBase
    {
        public RavenDB_10450(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Slow_IO_hints_are_stored_and_can_be_read()
        {
            var now = DateTime.UtcNow;
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                database.NotificationCenter
                    .SlowWrites
                    .Add(new IoChange()
                    {
                        FileName = "C:\\Raven", 
                        MeterItem = new IoMeterBuffer.MeterItem
                        {
                            Type = IoMetrics.MeterType.JournalWrite,
                            Size = 1024 * 1024,
                            Start = now,
                            End = now + TimeSpan.FromSeconds(10)
                        }
                    });

                database.NotificationCenter
                    .SlowWrites
                    .Add(new IoChange()
                    {
                        FileName = "C:\\Raven\\Indexes\\1",
                        MeterItem = new IoMeterBuffer.MeterItem
                        {
                            Type = IoMetrics.MeterType.JournalWrite,
                            Size = 1024 * 1024,
                            Start = now,
                            End = now + TimeSpan.FromSeconds(20)
                        }
                    });

                database.NotificationCenter
                    .SlowWrites
                    .Add(new IoChange()
                    {
                        FileName = "C:\\Raven",
                        MeterItem = new IoMeterBuffer.MeterItem
                        {
                            Type = IoMetrics.MeterType.JournalWrite,
                            Size = 2 * 1024 * 1024,
                            Start = now,
                            End = now + TimeSpan.FromSeconds(10)
                        }
                    });

                database.NotificationCenter.SlowWrites.UpdateNotificationInStorage(null);

                var details = database.NotificationCenter.SlowWrites
                    .GetSlowIoDetails();

                Assert.Equal(2, details.Writes.Count);
                Assert.Equal(1, details.Writes[$"{nameof(IoMetrics.MeterType.JournalWrite)}/C:\\Raven"].DataWrittenInMb);
                Assert.Equal(10, details.Writes[$"{nameof(IoMetrics.MeterType.JournalWrite)}/C:\\Raven"].DurationInSec);

                Assert.Equal(1, details.Writes[$"{nameof(IoMetrics.MeterType.JournalWrite)}/C:\\Raven\\Indexes\\1"].DataWrittenInMb);
                Assert.Equal(20, details.Writes[$"{nameof(IoMetrics.MeterType.JournalWrite)}/C:\\Raven\\Indexes\\1"].DurationInSec);

                database.NotificationCenter.SlowWrites.UpdateFrequency = TimeSpan.Zero;

                database.NotificationCenter
                    .SlowWrites
                    .Add(new IoChange()
                    {
                        FileName = "C:\\Raven",
                        MeterItem = new IoMeterBuffer.MeterItem
                        {
                            Type = IoMetrics.MeterType.JournalWrite,
                            Size = 2 * 1024 * 1024,
                            Start = now,
                            End = now + TimeSpan.FromSeconds(30)
                        }
                    });

                database.NotificationCenter.SlowWrites.UpdateNotificationInStorage(null);

                details = database.NotificationCenter.SlowWrites
                    .GetSlowIoDetails();

                Assert.Equal(2, details.Writes.Count);
                Assert.Equal(2, details.Writes[$"{nameof(IoMetrics.MeterType.JournalWrite)}/C:\\Raven"].DataWrittenInMb);
                Assert.Equal(30, details.Writes[$"{nameof(IoMetrics.MeterType.JournalWrite)}/C:\\Raven"].DurationInSec);
            }
        }
    }
}
