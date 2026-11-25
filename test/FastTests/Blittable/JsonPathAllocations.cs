using System;
using System.Text;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Blittable
{
    public unsafe class JsonPathAllocations(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        private const int NumberOfOperations = 4_000;

        private const string OrderJson =
            "{\"Company\":\"companies/17\",\"Employee\":\"employees/3\",\"OrderedAt\":\"2024-05-01T10:00:00.0000000Z\"," +
            "\"RequireAt\":\"2024-05-15T10:00:00.0000000Z\",\"ShippedAt\":null,\"ShipVia\":\"shippers/2\",\"Freight\":32.38," +
            "\"ShipTo\":{\"Line1\":\"12 alpha st.\",\"City\":\"lima\",\"PostalCode\":\"51100\",\"Country\":\"Israel\"}," +
            "\"Lines\":[" +
            "{\"Product\":\"products/11\",\"ProductName\":\"kilo 11\",\"PricePerUnit\":14.5,\"Quantity\":12,\"Discount\":0}," +
            "{\"Product\":\"products/42\",\"ProductName\":\"echo 42\",\"PricePerUnit\":9.8,\"Quantity\":10,\"Discount\":0.1}," +
            "{\"Product\":\"products/72\",\"ProductName\":\"tango 72\",\"PricePerUnit\":34.8,\"Quantity\":5,\"Discount\":0}]," +
            "\"@metadata\":{\"@collection\":\"Orders\"}}";

        [RavenFact(RavenTestCategory.Core)]
        public void Parsing_many_documents_in_one_context()
        {
            // a long-lived context (like a bulk insert) runs out of the pooled string values it hands out
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = Encoding.UTF8.GetBytes(OrderJson);
                var state = new JsonParserState();
                using (var parser = new UnmanagedJsonParser(context, state, "test"))
                using (var builder = new BlittableJsonDocumentBuilder(context, state, parser))
                fixed (byte* p = json)
                {
                    byte* buffer = p;
                    AssertAtMost(512, "Parse an order document", MeasurePerOperation(() =>
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
                    }));
                }
            }
        }

        private void AssertAtMost(int limit, string name, double bytesPerOperation)
        {
            Output.WriteLine($"{name,-40} {bytesPerOperation,8:F1} bytes/op");
#if !DEBUG
            Assert.True(bytesPerOperation <= limit, $"{name} allocated {bytesPerOperation:F1} bytes/op, expected at most {limit}");
#endif
        }

        private static double MeasurePerOperation(Action action)
        {
            for (int i = 0; i < 1_000; i++) // warm up so JIT and pool filling are excluded from the measurement
                action();

            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < NumberOfOperations; i++)
                action();

            return (double)(GC.GetAllocatedBytesForCurrentThread() - before) / NumberOfOperations;
        }
    }
}
