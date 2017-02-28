using System;
using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public abstract class AggregationResult : IDisposable
    {
        public abstract int Count { get; }

        public abstract IEnumerable<object> GetOutputs();

        public abstract IEnumerable<BlittableJsonReaderObject> GetOutputsToStore();
        
        public abstract void Dispose();
    }
}