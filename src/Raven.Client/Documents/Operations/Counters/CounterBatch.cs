using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.Counters
{
    public class CounterBatch 
    {
        public List<CounterOperation> Counters = new List<CounterOperation>();
    }

    public class CounterOperation 
    {
        public string DocumentId;
        public string CounterName;
        public long Delta; 
    }


    public class GetOrDeleteCounters
    {
        public List<CountersOperation> Counters = new List<CountersOperation>();
    }

    public class CountersOperation
    {
        public string DocumentId;
        public string[] Counters;
    }
}
