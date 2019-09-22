using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetDocumentsResult
    {
        public BlittableJsonReaderObject Includes { get; set; }

        public BlittableJsonReaderArray Results { get; set; }

        public BlittableJsonReaderObject CounterIncludes { get; set; }

        public BlittableJsonReaderObject TimeSeriesIncludes { get; set; }

        public int NextPageStart { get; set; }
    }
}
