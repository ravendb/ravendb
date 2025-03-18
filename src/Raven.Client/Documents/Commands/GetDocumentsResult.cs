using System;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public sealed class GetDocumentsResult
    {
        public BlittableJsonReaderObject Includes { get; set; }

        public BlittableJsonReaderArray Results { get; set; }

        public BlittableJsonReaderObject CounterIncludes { get; set; }
        
        public BlittableJsonReaderArray RevisionIncludes { get; set; }
        
        public BlittableJsonReaderObject TimeSeriesIncludes { get; set; }

        public BlittableJsonReaderObject CompareExchangeValueIncludes { get; set; }
    }
}
