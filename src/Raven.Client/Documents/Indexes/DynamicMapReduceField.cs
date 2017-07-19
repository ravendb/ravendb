namespace Raven.Client.Documents.Indexes
{
    public class DynamicMapReduceField
    {
        public string Name { get; set; }

        public string ClientSideName { get; set; }

        public AggregationOperation OperationType { get; set; }

        public bool IsGroupBy { get; set; }
    }
}