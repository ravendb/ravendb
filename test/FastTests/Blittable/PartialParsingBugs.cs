using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Blittable
{
    public class PartialParsingBugs : NoDisposalNeeded
    {
        [Theory]
        [InlineData("{\"Neg1\":-9223372036854775808,\"Neg\":-6}")]
        [InlineData("{\"Val\": Infinity}")]
        [InlineData("{\"Val\": -Infinity}")]
        [InlineData("{\"Val\": NaN}")]
        [InlineData("{\"Val\": false}")]
        [InlineData("{\"Val\": true}")]
        [InlineData("{\"Val\": -1}")]
        [InlineData("{\"Age\":6,\"Neg\":-6,\"IntMax\":2147483647}")]
        [InlineData("{\"Val\": null}")]
        [InlineData(
            "{\"TotalResults\":0,\"SkippedResults\":0,\"DurationInMs\":0,\"IndexName\":\"collection/Orders\",\"Results\":[],\"Includes\":[],\"IndexTimestamp\":\"0001-01-01T00:00:00.0000000\",\"LastQueryTime\":\"0001-01-01T00:00:00.0000000\",\"IsStale\":false,\"ResultEtag\":-4957615566507440024}")]
        public unsafe void TestOneCharAtATime(string s)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var oneByte = ParseJsonOneByteIncrements(s, ctx);
                var allAtOnce = ParseJsonAllAtOnce(s, ctx);
                Assert.Equal(allAtOnce, oneByte);
            }
        }

        private static unsafe BlittableJsonReaderObject ParseJsonOneByteIncrements(string s, JsonOperationContext ctx)
        {
            var jsonParserState = new JsonParserState();
            var parser = new UnmanagedJsonParser(ctx, jsonParserState, "test");
            var builder = new BlittableJsonDocumentBuilder(ctx, BlittableJsonDocumentBuilder.UsageMode.ToDisk, "test", parser, jsonParserState);
            builder.ReadObjectDocument();
            fixed (char* pStr = s)
            {
                var buffer = stackalloc byte[1];
                for (int i = 0; i < s.Length - 1; i++)
                {
                    buffer[0] = (byte)pStr[i];
                    parser.SetBuffer(buffer, 1);
                    Assert.False(builder.Read());
                }
                buffer[0] = (byte)pStr[s.Length - 1];
                parser.SetBuffer(buffer, 1);
                Assert.True(builder.Read());
            }
            builder.FinalizeDocument();
            var reader = builder.CreateReader();
            return reader;
        }

        private static unsafe BlittableJsonReaderObject ParseJsonAllAtOnce(string s, JsonOperationContext ctx)
        {
            var jsonParserState = new JsonParserState();
            var parser = new UnmanagedJsonParser(ctx, jsonParserState, "test");
            var builder = new BlittableJsonDocumentBuilder(ctx, BlittableJsonDocumentBuilder.UsageMode.ToDisk, "test", parser, jsonParserState);
            builder.ReadObjectDocument();
            var value = ctx.GetLazyString(s);
            parser.SetBuffer(value.Buffer, value.Size);
            Assert.True(builder.Read());
            builder.FinalizeDocument();
            var reader = builder.CreateReader();
            return reader;
        }
    }
}
