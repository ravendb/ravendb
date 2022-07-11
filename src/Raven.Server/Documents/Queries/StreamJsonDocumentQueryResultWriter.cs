using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class StreamJsonDocumentQueryResultWriter : AbstractDocumentStreamQueryResultWriter<Document>
    {
        public StreamJsonDocumentQueryResultWriter(Stream stream, JsonOperationContext context) : base(stream, context)
        {
        }

        private bool _first = true;

        public override async ValueTask AddResultAsync(Document res, CancellationToken token)
        {
            if (_first == false)
            {
                Writer.WriteComma();
            }
            else
            {
                _first = false;
            }
            
            Writer.WriteDocument(Context, res, metadataOnly: false);
            await Writer.MaybeFlushAsync(token);
        }
    }
}
