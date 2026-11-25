using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.BulkInsert;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents
{
    public class BulkInsertParsingAllocations(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        private const int NumberOfCommands = 6_000;
        private const int Warmup = 1_000;

        [RavenFact(RavenTestCategory.BulkInsert)]
        public async Task Reading_bulk_insert_commands_in_one_context()
        {
            var json = new StringBuilder("[");
            for (int i = 0; i < NumberOfCommands; i++)
            {
                if (i > 0)
                    json.Append(',');
                json.Append("{\"Id\":\"orders/").Append(i).Append("\",\"Type\":\"PUT\",\"Document\":")
                    .Append("{\"Company\":\"companies/17\",\"Employee\":\"employees/3\",\"Freight\":32.38,")
                    .Append("\"ShipTo\":{\"City\":\"lima\",\"Country\":\"Israel\"},\"Lines\":[{\"Product\":\"products/11\",\"Quantity\":12}],")
                    .Append("\"@metadata\":{\"@collection\":\"Orders\",\"Raven-Clr-Type\":\"Orders.Order, Orders\"}}}");
            }
            json.Append(']');

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (context.GetMemoryBuffer(out var buffer))
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(json.ToString())))
            using (var reader = new BulkInsertBatchCommandsReader(context, stream, buffer, CancellationToken.None))
            using (var modifier = new BlittableMetadataModifier(context))
            {
                await reader.InitAsync();

                long before = 0;
                for (int i = 0; i < NumberOfCommands; i++)
                {
                    if (i == Warmup)
                        before = GC.GetAllocatedBytesForCurrentThread();

                    // the reads complete synchronously from the memory stream, so they stay on this thread
                    var task = reader.GetCommandAsync(context, modifier);
                    Assert.True(task.IsCompleted);
                    var command = task.Result;
                    Assert.Equal(CommandType.PUT, command.Type);
                    command.Document.Dispose();
                    modifier.ReleaseAllocations();
                }

                var bytes = (double)(GC.GetAllocatedBytesForCurrentThread() - before) / (NumberOfCommands - Warmup);
                Output.WriteLine($"Read a bulk insert PUT command {bytes,8:F1} bytes/op");
#if !DEBUG
                Assert.True(bytes <= 1_600, $"Reading a command allocated {bytes:F1} bytes/op, expected at most 1,600");
#endif
            }
        }
    }
}
