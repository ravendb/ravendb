using Raven.Client.Documents.Operations.Counters;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Smuggler.Documents.Iteration
{
    public class CountersIterationState : CollectionAwareIterationState<CounterGroupDetail>
    {
        public CountersIterationState(DocumentsOperationContext context, Size pulseLimit) : base(context, pulseLimit)
        {
        }

        public override void OnMoveNext(CounterGroupDetail current)
        {
            if (StartEtagByCollection.Count != 0)
            {
                StartEtagByCollection[CurrentCollection] = current.Etag + 1;
            }
            else
            {
                StartEtag = current.Etag + 1;
            }

            ReadCount++;
        }
    }
}
