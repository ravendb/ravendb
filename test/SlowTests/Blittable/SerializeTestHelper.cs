using System.IO;
using System.Threading.Tasks;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace SlowTests.Blittable
{
    public static class SerializeTestHelper
    {
        public static async Task<BlittableJsonReaderObject> SimulateSavingToFileAndLoadingAsync(JsonOperationContext context, BlittableJsonReaderObject toStream)
        {
            //Simulates saving to file and loading
            BlittableJsonReaderObject fromStream;
            await using (Stream stream = new MemoryStream())
            {
                //Pass to stream
                await using (var textWriter = new AsyncBlittableJsonTextWriter(context, stream))
                {
                    context.Write(textWriter, toStream);
                }

                //Get from stream
                stream.Position = 0;

                var state = new JsonParserState();
                var parser = new UnmanagedJsonParser(context, state, "some tag");
                var peepingTomStream = new PeepingTomStream(stream, context);

                using (context.GetMemoryBuffer(out var buffer))
                using (var builder =
                    new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "some tag", parser, state))
                {
                    await UnmanagedJsonParserHelper.ReadAsync(peepingTomStream, parser, state, buffer);
                    await UnmanagedJsonParserHelper.ReadObjectAsync(builder, peepingTomStream, parser, buffer);

                    fromStream = builder.CreateReader();
                }
            }

            return fromStream;
        }
    }
}
