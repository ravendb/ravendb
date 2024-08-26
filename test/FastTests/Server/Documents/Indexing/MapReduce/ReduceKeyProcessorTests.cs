using Raven.Client.Util;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.ServerWide;
using Sparrow.Server;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Threading;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Indexing.MapReduce
{
    public class ReduceKeyProcessorTests : NoDisposalNeeded
    {
        public ReduceKeyProcessorTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_handle_values_of_different_types()
        {
            using (var bufferPool = new UnmanagedBuffersPoolWithLowMemoryHandling(RavenLogManager.CreateNullLogger(), "ReduceKeyProcessorTests"))
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var bsc = new ByteStringContext(SharedMultipleUseFlag.None))
            {
                var sut = new ReduceKeyProcessor(9, bufferPool, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion);

                sut.Reset();

                sut.Process(bsc, 1);
                sut.Process(bsc, (long)1);
                sut.Process(bsc, 1.0);
                sut.Process(bsc, (decimal)1.0);
                sut.Process(bsc, "abc");
                using (var lazyStringValue = context.GetLazyString("abc"))
                {
                    sut.Process(bsc, lazyStringValue);
                }
                sut.Process(bsc, context.ReadObject(new DynamicJsonValue()
                {
                    ["Name"] = "Arek",
                    ["Age"] = null
                }, "foo"));

                sut.Process(bsc, new DynamicJsonArray()
                {
                    1,
                    2,
                    null,
                    3
                });

                sut.Process(bsc, SystemTime.UtcNow);

                Assert.NotEqual((ulong)0, sut.Hash);
                sut.ReleaseBuffer();
            }
        }
    }
}
