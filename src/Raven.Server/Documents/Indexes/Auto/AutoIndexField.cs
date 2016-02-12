using Raven.Abstractions.Indexing;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoIndexField
    {
        public AutoIndexField(string name, SortOptions? sortOption, bool highlighted)
        {
            Name = name;
            SortOption = sortOption;
            Highlighted = highlighted;
        }

        public string Name { get; }

        public SortOptions? SortOption { get; }

        public bool Highlighted { get; }
    }
}