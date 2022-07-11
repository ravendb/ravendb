using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class StreamBlittableDocumentQueryResultWriter : AbstractDocumentStreamQueryResultWriter<BlittableJsonReaderObject>
    {
        private bool _first = true;

        public StreamBlittableDocumentQueryResultWriter(Stream stream, JsonOperationContext context) : base(stream, context)
        {
        }

        public override async ValueTask AddResultAsync(BlittableJsonReaderObject res, CancellationToken token)
        {
            if (_first == false)
            {
                Writer.WriteComma();
            }
            else
            {
                _first = false;
            }

            Writer.WriteObject(res);
            await Writer.MaybeFlushAsync(token);
        }
    }
}
