using System.IO;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace SlowTests.Blittable
{
    public class SerializeTestHelper
    {
        public static BlittableJsonReaderObject SimulateSavingToFileAndLoading(JsonOperationContext context, BlittableJsonReaderObject toStream)
        {
            //Simulates saving to file and loading
            BlittableJsonReaderObject fromStream;
            using (Stream stream = new MemoryStream())
            {
                //Pass to stream
                using (var textWriter = new BlittableJsonTextWriter(context, stream))
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
                    UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer);
                    UnmanagedJsonParserHelper.ReadObject(builder, peepingTomStream, parser, buffer);

                    fromStream = builder.CreateReader();
                }
            }

            return fromStream;
        }
    }
}
