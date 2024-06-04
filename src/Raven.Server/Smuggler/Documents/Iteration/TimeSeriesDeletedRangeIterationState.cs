using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Smuggler.Documents.Iteration
{
    public sealed class TimeSeriesDeletedRangeIterationState : CollectionAwareIterationState<TimeSeriesDeletedRangeItemForSmuggler>
    {
        public TimeSeriesDeletedRangeIterationState(DocumentsOperationContext context, Size pulseLimit) : base(context, pulseLimit)
        {
        }

        public override void OnMoveNext(TimeSeriesDeletedRangeItemForSmuggler current)
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
