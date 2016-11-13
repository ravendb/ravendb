using Raven.Abstractions.Indexing;

namespace Raven.NewClient.Client.Indexing
{
    public class DynamicMapReduceField
    {
        public string Name { get; set; }

        public string ClientSideName { get; set; }

        public FieldMapReduceOperation OperationType { get; set; }

        public bool IsGroupBy { get; set; }
    }
}