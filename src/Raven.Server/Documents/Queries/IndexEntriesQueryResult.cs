using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents.Queries.Explanation;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class IndexEntriesQueryResult : QueryResultServerSide<BlittableJsonReaderObject>
    {
        public static readonly IndexEntriesQueryResult NotModifiedResult = new IndexEntriesQueryResult { NotModified = true };

        public override void AddResult(BlittableJsonReaderObject result)
        {
            Results.Add(result);
        }

        public override void AddHighlightings(Dictionary<string, Dictionary<string, string[]>> highlightings)
        {
            throw new NotSupportedException();
        }

        public override void AddExplanation(ExplanationResult explanationResult)
        {
            throw new NotSupportedException();
        }

        public override void HandleException(Exception e)
        {
            throw new NotSupportedException();
        }

        public override void AddCounterIncludes(Dictionary<string, List<CounterDetail>> dictionary)
        {
            throw new NotSupportedException();
        }

        public override Dictionary<string, List<CounterDetail>> GetCounterIncludes()
        {
            throw new NotSupportedException();
        }

        public override bool SupportsExceptionHandling => false;
        public override bool SupportsInclude => false;
        public override bool SupportsHighlighting => false;
        public override bool SupportsExplanations => false;

    }
}
