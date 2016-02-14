using Raven.Server.Json;
using Voron.Data.Tables;

namespace Raven.Server.Indexing.Corax.Queries
{
    public abstract class Query
    {
        protected FullTextIndex Index;
        protected RavenOperationContext Context;
        protected Table IndexEntries;

        public float Boost { get; set; }

        protected Query()
        {
            Boost = 1.0f;
        }

        public void Initialize(FullTextIndex index, RavenOperationContext context, Table entries)
        {
            Index = index;
            Context = context;
            IndexEntries = entries;
            Init();
        }

        protected abstract void Init();
        public abstract QueryMatch[] Execute();

        public abstract override string ToString();
    }
}