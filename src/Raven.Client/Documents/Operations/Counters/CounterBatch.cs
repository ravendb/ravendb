using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.Counters
{
    public class CounterBatch
    {
        public bool ReplyWithAllNodesValues;
        public List<DocumentCountersOperation> Documents;
    }

    public class DocumentCountersOperation
    {
        public List<CounterOperation> Operations;
        public string DocumentId;
    }

    public enum CounterOperationType
    {
        None,
        Increment,
        Delete,
        Get
    }

    public class CounterOperation
    {
        public CounterOperationType Type;
        public string CounterName;
        public long Delta; 
    }
}
