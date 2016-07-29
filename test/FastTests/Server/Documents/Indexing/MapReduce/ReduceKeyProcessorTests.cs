using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.Static;
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
            using (var context = new JsonOperationContext(bufferPool))
            {
                var sut = new ReduceKeyProcessor(10, bufferPool);

                sut.Init();

                sut.Process(1);
                sut.Process((long)1);
                sut.Process(1.0);
                sut.Process((decimal)1.0);
                sut.Process("abc");
                sut.Process(context.GetLazyString("abc"));
                sut.Process(new DynamicBlittableJson(context.ReadObject(new DynamicJsonValue()
                {
                    ["Name"] = "Arek"
                }, "foo")));

                Assert.NotEqual((ulong)0, sut.Hash);
            }
        }
    }
}