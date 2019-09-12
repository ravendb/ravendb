using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Enumerators;
using Sparrow;

namespace Raven.Server.Smuggler.Documents.Iteration
{
    public abstract class CollectionAwareIterationState<T> : PulsedEnumerationState<T>
    {
        protected CollectionAwareIterationState(DocumentsOperationContext context, Size pulseLimit) : base(context, pulseLimit)
        {
        }

        public long StartEtag;

        public Dictionary<string, long> StartEtagByCollection = new Dictionary<string, long>();

        public string CurrentCollection;
    }
}
