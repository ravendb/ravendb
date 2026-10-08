using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27670 : NoDisposalNeeded
    {
        public RavenDB_27670(ITestOutputHelper output) : base(output)
        {
        }

        // Needs ~2.3 GB of RAM: the writer's in-memory buffer has to grow past int.MaxValue bytes.
        [RavenMultiplatformFact(RavenTestCategory.Core | RavenTestCategory.Smuggler, RavenArchitecture.AllX64, NightlyBuildRequired = true)]
        public async Task MaybeFlushAsync_Should_Flush_When_Buffer_Exceeds_Int_MaxValue()
        {
            var chunk = new byte[64 * 1024 * 1024];
            chunk.AsSpan().Fill((byte)'a');

            using (var context = JsonOperationContext.ShortTermSingleUse())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, Stream.Null))
            {
                // One "document" of 33 x 64 MiB = 2,214,592,512 bytes, with no flush inside it
                for (var i = 0; i < 33; i++)
                    writer.WriteRawString(chunk);

                var flushed = await writer.MaybeFlushAsync();

                Assert.Equal(33L * chunk.Length, flushed);
            }
        }
    }
}
