using System;
using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.Documents.Indexes.MapReduce.Static;

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