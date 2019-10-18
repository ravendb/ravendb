using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Enumerators;
using Sparrow;

namespace Raven.Server.Documents.Indexes
{
    public class QueryResultsIterationState : PulsedEnumerationState<IndexReadOperation.QueryResult>
    {
        public QueryResultsIterationState(DocumentsOperationContext context, Size pulseLimit) : base(context, pulseLimit)
        {
        }

        public override void OnMoveNext(IndexReadOperation.QueryResult current)
        {
            ReadCount++;
        }
    }
}
