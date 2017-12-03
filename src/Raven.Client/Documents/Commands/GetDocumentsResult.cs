using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetDocumentsResult
    {
        public BlittableJsonReaderObject Includes { get; set; }

        public BlittableJsonReaderArray Results { get; set; }

        public int NextPageStart { get; set; }
    }
}
