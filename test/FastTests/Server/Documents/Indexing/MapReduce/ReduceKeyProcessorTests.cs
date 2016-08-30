using Raven.Abstractions;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Indexing.MapReduce
{
    public class ReduceKeyProcessorTests
    {
        [Fact]
        public void Can_handle_values_of_different_types()
        {
            using (var bufferPool = new UnmanagedBuffersPool("ReduceKeyProcessorTests"))
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var sut = new ReduceKeyProcessor(10, bufferPool);

                sut.Reset();

                sut.Process(1);
                sut.Process((long)1);
                sut.Process(1.0);
                sut.Process((decimal)1.0);
                sut.Process("abc");
                using (var lazyStringValue = context.GetLazyString("abc"))
                {
                    sut.Process(lazyStringValue);
                }
                sut.Process(new DynamicBlittableJson(context.ReadObject(new DynamicJsonValue()
                {
                    ["Name"] = "Arek",
                    ["Age"] = null
                }, "foo")));

                sut.Process(new DynamicArray(new DynamicJsonArray()
                {
                    1,
                    2,
                    null,
                    3
                }));

                sut.Process(SystemTime.UtcNow);

                Assert.NotEqual((ulong)0, sut.Hash);
            }
        }
    }
}