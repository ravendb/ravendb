using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Enumerators;
using Sparrow;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class CollectionQueryResultsIterationState : PulsedEnumerationState<Document>
    {
        public CollectionQueryResultsIterationState(DocumentsOperationContext context, Size pulseLimit) : base(context, pulseLimit)
        {
        }

        public int Start;
        public int Take;

        public override void OnMoveNext(Document current)
        {
            ReadCount++;

            Start++;
            Take--;
        }
    }
}
