using Raven.Abstractions.Indexing;

namespace Raven.Client.Indexing
{
    public class DynamicMapReduceField
    {
        public string Name { get; set; }

        public FieldMapReduceOperation OperationType { get; set; }

        public bool IsGroupBy { get; set; }
    }
}