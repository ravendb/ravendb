using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Server.Meters;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20039 : RavenTestBase
{
    public RavenDB_20039(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Slow_IO_hints_are_stored_for_all_IO_types()
    {
        var now = DateTime.UtcNow;
        using (var store = GetDocumentStore(new Options
        {
            RunInMemory = false
        }))
        {
            var database = await GetDatabase(Server, store.Database);

            // We want to store a PerformanceHint in which there would be a SlowIoDetails.SlowWriteInfo in the details,
            // with the missing "type" property, in order to ensure that the handling of records prior to implementing
            // the new key structure for such records is done correctly.
            GenerateLegacySlowWritesEntryInStorage(database, now);

            database.NotificationCenter
                .SlowWrites
                .Add(new IoChange
                {
                    FileName = "C:\\Raven\\Compression",
                    MeterItem = new IoMeterBuffer.MeterItem
                    {
                        Type = IoMetrics.MeterType.Compression,
                        Size = 2 * 1024 * 1024,
                        Start = now,
                        End = now + TimeSpan.FromSeconds(1)
                    }
                });

            database.NotificationCenter
                .SlowWrites
                .Add(new IoChange
                {
                    FileName = "C:\\Raven\\DataFlush",
                    MeterItem = new IoMeterBuffer.MeterItem
                    {
                        Type = IoMetrics.MeterType.DataFlush,
                        Size = 3 * 1024 * 1024,
                        Start = now,
                        End = now + TimeSpan.FromSeconds(1)
                    }
                });

            database.NotificationCenter
                .SlowWrites
                .Add(new IoChange
                {
                    FileName = "C:\\Raven\\DataSync",
                    MeterItem = new IoMeterBuffer.MeterItem
                    {
                        Type = IoMetrics.MeterType.DataSync,
                        Size = 4 * 1024 * 1024,
                        Start = now,
                        End = now + TimeSpan.FromSeconds(1)
                    }
                });

            database.NotificationCenter
                .SlowWrites
                .Add(new IoChange
                {
                    FileName = "C:\\Raven\\JournalWrite",
                    MeterItem = new IoMeterBuffer.MeterItem
                    {
                        Type = IoMetrics.MeterType.JournalWrite,
                        Size = 5 * 1024 * 1024,
                        Start = now,
                        End = now + TimeSpan.FromSeconds(1)
                    }
                });

            database.NotificationCenter.SlowWrites.UpdateNotificationInStorage(null);

            var details = database.NotificationCenter
                .SlowWrites.GetSlowIoDetails();

            Assert.Equal(5, details.Writes.Count);

            Assert.Equal(1, details.Writes[$"{nameof(IoMetrics.MeterType.JournalWrite)}/C:\\Raven\\LegacyEntry"].DataWrittenInMb);
            Assert.Equal(2, details.Writes[$"{nameof(IoMetrics.MeterType.Compression)}/C:\\Raven\\Compression"].DataWrittenInMb);
            Assert.Equal(3, details.Writes[$"{nameof(IoMetrics.MeterType.DataFlush)}/C:\\Raven\\DataFlush"].DataWrittenInMb);
            Assert.Equal(4, details.Writes[$"{nameof(IoMetrics.MeterType.DataSync)}/C:\\Raven\\DataSync"].DataWrittenInMb);
            Assert.Equal(5, details.Writes[$"{nameof(IoMetrics.MeterType.JournalWrite)}/C:\\Raven\\JournalWrite"].DataWrittenInMb);
        }
    }

    private static void GenerateLegacySlowWritesEntryInStorage(DocumentDatabase database, DateTime now)
    {
        const string path = "C:\\Raven\\LegacyEntry";
        var details = new SlowIoDetails();
        var ioChange = new IoChange
        {
            FileName = path,
            MeterItem = new IoMeterBuffer.MeterItem
            {
                Size = 1 * 1024 * 1024,
                Start = now,
                End = now + TimeSpan.FromSeconds(1)
            }
        };

        var slowWriteInfo = new SlowIoDetails.SlowWriteInfo(ioChange, now);

        details.Writes.Add(path, slowWriteInfo);

        var hint = PerformanceHint.Create(
            database.Name,
            "An extremely slow write to disk",
            "We have detected very slow writes",
            PerformanceHintType.SlowIO,
            NotificationSeverity.Info,
            "slow-writes",
            details
        );

        using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var json = context.ReadObject(hint.ToJson(), "notification", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
        {
            json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject bjroDetails);
            bjroDetails.TryGet(nameof(SlowIoDetails.Writes), out BlittableJsonReaderObject bjroWrites);
            bjroWrites.TryGet(path, out BlittableJsonReaderObject bjroSlowWriteInfo);

            var propertyIndex = bjroSlowWriteInfo.GetPropertyIndex(nameof(SlowIoDetails.SlowWriteInfo.Type));

            bjroSlowWriteInfo.Modifications = new DynamicJsonValue(bjroSlowWriteInfo) { Removals = new HashSet<int> { propertyIndex } };

            using (var hintWithLegacySlowWriteInfo = context.ReadObject(json, "notification", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
            {
                hintWithLegacySlowWriteInfo.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject bjroLegacyDetails);
                bjroLegacyDetails.TryGet(nameof(SlowIoDetails.Writes), out BlittableJsonReaderObject bjroLegacyWrites);
                bjroLegacyWrites.TryGet(path, out BlittableJsonReaderObject bjroLegacySlowWriteInfo);

                Assert.Equal(-1, bjroLegacySlowWriteInfo.GetPropertyIndex(nameof(SlowIoDetails.SlowWriteInfo.Type)));

                using (var tx = context.OpenWriteTransaction())
                {
                    database.NotificationCenter.Storage.Store(context.GetLazyString(hint.Id), hint.CreatedAt, null, hintWithLegacySlowWriteInfo, tx);
                    tx.Commit();
                }
            }
        }
    }
}
