using System.Buffers;
using System.IO;
using System.Text;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_27502 : NoDisposalNeeded
    {
        public RavenDB_27502(ITestOutputHelper output) : base(output)
        {
        }

        private const string StaleMarker = "STALE-POOLED-BYTES";

        [RavenFact(RavenTestCategory.Core)]
        public void ParserErrorForInvalidJsonStringMustNotContainStalePooledMemory()
        {
            const string invalidJson = "Beverages include soft drinks.";

            using (JsonOperationContext context = JsonOperationContext.ShortTermSingleUse())
            {
                PoisonPooledBufferFor(invalidJson, StaleMarker);

                InvalidDataException e = Assert.Throws<InvalidDataException>(() => context.Sync.ReadForMemory(invalidJson, "test"));

                Assert.Contains(invalidJson, e.Message);
                Assert.DoesNotContain(StaleMarker, e.Message);
                Assert.DoesNotContain('\0', e.Message);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void TruncatedJsonStringMustNotBeCompletedByStalePooledMemory()
        {
            const string truncatedJson = "{\"Name\":\"Oren\"";

            using (JsonOperationContext context = JsonOperationContext.ShortTermSingleUse())
            {
                // the pooled array is filled with '}' so the parser would close the object from stale bytes
                PoisonPooledBufferFor(truncatedJson, "}");

                Assert.Throws<EndOfStreamException>(() => context.Sync.ReadForMemory(truncatedJson, "test"));
            }
        }

        private static void PoisonPooledBufferFor(string json, string filler)
        {
            // ReadForMemory(string) rents from ArrayPool<byte>.Shared using the max UTF-8 byte count of the input.
            // Returning an array of that size class on this thread makes the next Rent of the same size class on
            // this thread hand back this exact array, so its content is what a previous user of the pool left there.
            byte[] buffer = ArrayPool<byte>.Shared.Rent(Encoding.UTF8.GetMaxByteCount(json.Length));
            byte[] fillerBytes = Encoding.UTF8.GetBytes(filler);
            for (int i = 0; i < buffer.Length; i++)
                buffer[i] = fillerBytes[i % fillerBytes.Length];

            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
