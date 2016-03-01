using Raven.Abstractions.Indexing;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicSortInfo
    {
        public string Field { get; set; }
        public SortOptions FieldType { get; set; }
    }
}