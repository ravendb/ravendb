using System.Collections.Generic;
using Raven.Server.Documents.Queries.Explanation;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Enumerators;
using Sparrow;

namespace Raven.Server.Documents.Indexes
{
    public class QueryResultsIterationState : PulsedEnumerationState<(Document Result, Dictionary<string, Dictionary<string, string[]>> Highlightings, ExplanationResult Explanation)>
    {
        public QueryResultsIterationState(DocumentsOperationContext context, Size pulseLimit) : base(context, pulseLimit)
        {
        }

        public override void OnMoveNext((Document Result, Dictionary<string, Dictionary<string, string[]>> Highlightings, ExplanationResult Explanation) current)
        {
            ReadCount++;
        }
    }
}
