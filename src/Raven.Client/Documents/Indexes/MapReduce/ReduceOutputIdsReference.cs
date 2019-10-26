using System.Collections.Generic;

namespace Raven.Client.Documents.Indexes.MapReduce
{
    public class ReduceOutputIdsReference
    {
        public string Id { get; set; }

        public List<string> ReduceOutputs { get; set; }
    }
}
