using Raven.Abstractions.Indexing;
using Raven.Server.Documents.Indexes;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicSortInfo
    {
        private string _normalizedName;

        public string Name { get; set; }

        public string NormalizedName => _normalizedName ?? (_normalizedName = IndexField.ReplaceInvalidCharactersInFieldName(Name));

        public SortOptions FieldType { get; set; }
    }
}