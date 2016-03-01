using Raven.Abstractions.Indexing;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoIndexField : IndexField
    {
        public AutoIndexField(string name, SortOptions? sortOption = null, bool highlighted = false)
        {
            Name = name;
            SortOption = sortOption;
            Highlighted = highlighted;
        }

        public override FieldStorage Storage => FieldStorage.No;

        public override FieldIndexing Indexing => FieldIndexing.Default;
    }
}