using System.Text;
using Raven.Server.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Blittable.BlittableJsonWriterTests
{
    public unsafe class BlittableMetadataModifierTest : NoDisposalNeeded
    {
        [Fact]
        public void BlittableMetadataModifier_WhileIdContainsNoEscapeCharacters_ResultInLazyStringWithoutEscapeInformation()
        {
            const string json = "{\"@metadata\": { \"@id\": \"u1\"}}";

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var buffer = Encoding.UTF8.GetBytes(json);
                var state = new JsonParserState();

                var modifier = new BlittableMetadataModifier(ctx);
                using (var parser = new UnmanagedJsonParser(ctx, state, "test"))
                    fixed (byte* pBuffer = buffer)
                    {
                        parser.SetBuffer(pBuffer, buffer.Length);

                        using (
                            var builder = new BlittableJsonDocumentBuilder(ctx,
                                BlittableJsonDocumentBuilder.UsageMode.None, "test", parser, state, null, modifier))
                        {
                            builder.ReadObjectDocument();
                            builder.Read();
                            builder.FinalizeDocument();
                        }
                    }

                Assert.NotNull(modifier.Id.EscapePositions);
            }
        }
    }
}
