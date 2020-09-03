using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Smuggler.Documents.Iteration
{
    public class TimeSeriesIterationState : CollectionAwareIterationState<TimeSeriesItem>
    {
        public TimeSeriesIterationState(DocumentsOperationContext context, Size pulseLimit) : base(context, pulseLimit)
        {
        }

        public override void OnMoveNext(TimeSeriesItem current)
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
