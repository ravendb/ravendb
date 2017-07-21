using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicSortInfo
    {
        public string Name { get; set; }

        public SortOptions FieldType { get; set; }
    }
}