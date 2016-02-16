using Raven.Abstractions.Indexing;

namespace Raven.Server.Documents.Indexes
{
    public abstract class IndexField
    {
        public string Name { get; protected set; }

        public SortOptions? SortOption { get; protected set; }

        public bool Highlighted { get; protected set; }

        public abstract FieldStorage Storage { get; }

        public abstract FieldIndexing Indexing { get; }
    }
}