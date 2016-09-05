using Raven.Abstractions.Indexing;
using Raven.Server.Documents.Indexes;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryMappingItem
    {
        private string _normalizedName;

        public DynamicQueryMappingItem(string name, FieldMapReduceOperation mapReduceOperation)
        {
            Name = name;
            MapReduceOperation = mapReduceOperation;
        }

        // TODO arek - names of this props are not really expressive, let me commment it for now and introduce single Name
        //public string QueryFrom { get; set; }
        //public string From { get; set; }
        //public string To { get; set; }
        public string Name { get; }

        public string NormalizedName => _normalizedName ?? (_normalizedName = IndexField.ReplaceInvalidCharactersInFieldName(Name));

        public FieldMapReduceOperation MapReduceOperation { get; }
    }
}