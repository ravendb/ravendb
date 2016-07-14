using System;
using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class AggregationResult : IDisposable
    {
        public readonly List<BlittableJsonReaderObject> Outputs;

        public AggregationResult(List<BlittableJsonReaderObject> results)
        {
            Outputs = results;
        }

        public void Dispose()
        {
            foreach (var item in Outputs)
            {
                item.Dispose();
            }
        }
    }
}