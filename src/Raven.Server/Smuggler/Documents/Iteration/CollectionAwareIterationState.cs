using System.Collections.Generic;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Smuggler.Documents.Iteration
{
    public abstract class CollectionAwareIterationState<T> : IterationState<T>
    {
        protected CollectionAwareIterationState(DocumentsOperationContext context) : base(context)
        {
        }

        public long StartEtag;

        public Dictionary<string, long> StartEtagByCollection = new Dictionary<string, long>();

        public string CurrentCollection;
    }
}
