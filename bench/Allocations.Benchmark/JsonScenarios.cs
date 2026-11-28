using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.BulkInsert;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using Xunit;
using ServerWriterExtensions = Raven.Server.Json.BlittableJsonTextWriterExtensions;

namespace Allocations.Benchmark
{
    // Parsing, reading, comparing and writing blittable documents
    public class JsonScenarios
    {
        private const string OrderJson =
            "{\"Company\":\"companies/17\",\"Employee\":\"employees/3\",\"OrderedAt\":\"2024-05-01T10:00:00.0000000Z\"," +
            "\"RequireAt\":\"2024-05-15T10:00:00.0000000Z\",\"ShippedAt\":null,\"ShipVia\":\"shippers/2\",\"Freight\":32.38," +
            "\"ShipTo\":{\"Line1\":\"12 alpha st.\",\"City\":\"lima\",\"PostalCode\":\"51100\",\"Country\":\"Israel\"}," +
            "\"Lines\":[" +
            "{\"Product\":\"products/11\",\"ProductName\":\"kilo 11\",\"PricePerUnit\":14.5,\"Quantity\":12,\"Discount\":0}," +
            "{\"Product\":\"products/42\",\"ProductName\":\"echo 42\",\"PricePerUnit\":9.8,\"Quantity\":10,\"Discount\":0.1}," +
            "{\"Product\":\"products/72\",\"ProductName\":\"tango 72\",\"PricePerUnit\":34.8,\"Quantity\":5,\"Discount\":0}]," +
            "\"@metadata\":{\"@collection\":\"Orders\"}}";

        // a long-lived context (like a bulk insert) runs out of the pooled string values it hands out
        public unsafe void ParseDocumentsInOneContext()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = Encoding.UTF8.GetBytes(OrderJson);
                var state = new JsonParserState();
                using (var parser = new UnmanagedJsonParser(context, state, "test"))
                using (var builder = new BlittableJsonDocumentBuilder(context, state, parser))
                fixed (byte* p = json)
                {
                    byte* buffer = p;
                    Measure.Report("Parse an order document", Measure.PerOperation(() =>
                    {
                        state.Reset();
                        parser.NewDocument();
                        parser.SetBuffer(buffer, json.Length);
                        builder.Renew("test", BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                        context.CachedProperties.NewDocument();
                        builder.ReadObjectDocument();
                        Assert.True(builder.Read());
                        builder.FinalizeDocument();
                        using (builder.CreateReader())
                        {
                        }
                    }, operations: 4_000));
                }
            }
        }

        public unsafe void ReadArrayItemsByIndex()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var document = context.Sync.ReadForMemory(OrderJson, "order"))
            {
                var prop = new BlittableJsonReaderObject.PropertyDetails();
                Measure.Report("Read the order lines by index", Measure.PerOperation(() =>
                {
                    // a new reader each time, like a document read for indexing
                    using (var order = new BlittableJsonReaderObject(document.BasePointer, document.Size, context))
                    {
                        Assert.True(order.TryGet("Lines", out BlittableJsonReaderArray lines));
                        for (int i = 0; i < lines.Length; i++)
                        {
                            var line = (BlittableJsonReaderObject)lines[i];
                            line.GetPropertyByIndex(0, ref prop);
                            Assert.Same(line, lines[i]);
                        }
                    }
                }, operations: 4_000));
            }
        }

        public void CompareUpdatedDocument()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var current = context.Sync.ReadForMemory(OrderJson, "current"))
            using (var modified = context.Sync.ReadForMemory(OrderJson.Replace("32.38", "33.38"), "modified"))
            {
                Measure.Report("DocumentCompare.IsEqualTo, one changed property", Measure.PerOperation(() =>
                    Assert.Equal(DocumentCompareResult.NotEqual,
                        DocumentCompare.IsEqualTo(current, modified, DocumentCompare.DocumentCompareOptions.MergeMetadataAndThrowOnAttachmentModification)),
                    operations: 10_000));
            }
        }

        public void WriteDocuments()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var orders = new Document[64];
                for (int i = 0; i < orders.Length; i++)
                    orders[i] = CreateOrder(context, i);

                var writer = new AsyncBlittableJsonTextWriter(context, Stream.Null);
                try
                {
                    var i1 = 0;
                    Measure.Report("WriteObject, order", Measure.PerOperation(() =>
                    {
                        writer.WriteObject(orders[i1++ % orders.Length].Data);
                        writer.MaybeFlushAsync().AsTask().GetAwaiter().GetResult();
                    }));

                    var i2 = 0;
                    Measure.Report("WriteDocument, order", Measure.PerOperation(() =>
                    {
                        ServerWriterExtensions.WriteDocument(writer, context, orders[i2++ % orders.Length], metadataOnly: false);
                        writer.MaybeFlushAsync().AsTask().GetAwaiter().GetResult();
                    }));

                    var i3 = 0;
                    Measure.Report("WriteDocument, order metadata", Measure.PerOperation(() =>
                    {
                        ServerWriterExtensions.WriteDocument(writer, context, orders[i3++ % orders.Length], metadataOnly: true);
                        writer.MaybeFlushAsync().AsTask().GetAwaiter().GetResult();
                    }));
                }
                finally
                {
                    writer.DisposeAsync().AsTask().GetAwaiter().GetResult();
                }
            }
        }

        public async Task ReadBulkInsertCommandsInOneContext()
        {
            const int warmup = 1_000, count = 5_000;

            var json = new StringBuilder("[");
            for (int i = 0; i < warmup + count; i++)
            {
                if (i > 0)
                    json.Append(',');
                json.Append("{\"Id\":\"orders/").Append(i).Append("\",\"Type\":\"PUT\",\"Document\":").Append(OrderJson).Append('}');
            }
            json.Append(']');

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (context.GetMemoryBuffer(out var buffer))
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(json.ToString())))
            using (var reader = new BulkInsertBatchCommandsReader(context, stream, buffer, CancellationToken.None))
            using (var modifier = new BlittableMetadataModifier(context))
            {
                await reader.InitAsync();

                Measure.Report("Read a bulk insert PUT command", Measure.PerOperation(() =>
                {
                    // the reads complete synchronously from the memory stream, so they stay on this thread
                    var task = reader.GetCommandAsync(context, modifier);
                    Assert.True(task.IsCompleted);
                    var command = task.Result;
                    Assert.Equal(CommandType.PUT, command.Type);
                    command.Document.Dispose();
                    modifier.ReleaseAllocations();
                }, operations: count, warmup: warmup));
            }
        }

        private static Document CreateOrder(JsonOperationContext context, int i)
        {
            var lines = new DynamicJsonArray();
            for (int j = 0; j <= i % 8; j++)
            {
                lines.Add(new DynamicJsonValue
                {
                    ["Discount"] = j % 3 == 0 ? 0.15 : 0,
                    ["PricePerUnit"] = 14.5 + j,
                    ["Product"] = $"products/{11 + j}-A",
                    ["ProductName"] = "Queso Cabrales",
                    ["Quantity"] = 12 + j
                });
            }

            var data = context.ReadObject(new DynamicJsonValue
            {
                ["Company"] = $"companies/{85 + i}-A",
                ["Employee"] = "employees/5-A",
                ["Freight"] = 32.38 + i,
                ["IsPaid"] = i % 2 == 0,
                ["Lines"] = lines,
                ["Notes"] = "Leave at the back door, ring twice",
                ["OrderedAt"] = "1996-07-04T00:00:00.0000000",
                ["RequireAt"] = "1996-08-01T00:00:00.0000000",
                ["ShipTo"] = new DynamicJsonValue
                {
                    ["City"] = "Reims",
                    ["Country"] = "France",
                    ["Line1"] = "59 rue de l'Abbaye",
                    ["Line2"] = null,
                    ["Location"] = new DynamicJsonValue { ["Latitude"] = 49.2583, ["Longitude"] = 4.0317 },
                    ["PostalCode"] = "51100",
                    ["Region"] = null
                },
                ["ShipVia"] = "shippers/3-A",
                ["ShippedAt"] = "1996-07-16T00:00:00.0000000",
                ["Status"] = "Shipped",
                ["Total"] = 440.5 + i,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Orders",
                    [Constants.Documents.Metadata.RavenClrType] = "Orders.Order, Northwind"
                }
            }, $"orders/{i}-A");

            return new Document
            {
                Data = data,
                Id = context.GetLazyString($"orders/{i}-A"),
                ChangeVector = $"A:{1000 + i}-hSUUlkkaJUaqWHHGsnLbMw",
                LastModified = new DateTime(2026, 11, 23, 10, 0, i % 60, DateTimeKind.Utc),
                Flags = DocumentFlags.HasRevisions
            };
        }
    }
}
