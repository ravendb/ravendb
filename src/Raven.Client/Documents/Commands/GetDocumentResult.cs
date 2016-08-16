using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetDocumentResult
    {
        public BlittableJsonReaderObject[] Includes { get; set; }

        public BlittableJsonReaderObject[] Results { get; set; }
    }
}