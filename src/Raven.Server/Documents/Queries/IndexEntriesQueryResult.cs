using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Explanation;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class IndexEntriesQueryResult : QueryResultServerSide<BlittableJsonReaderObject>
    {
        public static readonly IndexEntriesQueryResult NotModifiedResult = new IndexEntriesQueryResult { NotModified = true };

        public override ValueTask AddResultAsync(BlittableJsonReaderObject result, CancellationToken token)
        {
            Results.Add(result);
            return default;
        }

        public override void AddHighlightings(Dictionary<string, Dictionary<string, string[]>> highlightings)
        {
            throw new NotSupportedException();
        }

        public override void AddExplanation(ExplanationResult explanationResult)
        {
            throw new NotSupportedException();
        }

        public override ValueTask HandleExceptionAsync(Exception e, CancellationToken token)
        {
            return default;
        }

        public override void AddCounterIncludes(IncludeCountersCommand includeCountersCommand)
        {
            throw new NotSupportedException();
        }

        public override Dictionary<string, List<CounterDetail>> GetCounterIncludes()
        {
            throw new NotSupportedException();
        }

        public override void AddTimeSeriesIncludes(ITimeSeriesIncludes includeTimeSeriesCommand)
        {
            throw new NotSupportedException();
        }

        public override ITimeSeriesIncludes GetTimeSeriesIncludes()
        {
            throw new NotSupportedException();
        }

        public override void AddCompareExchangeValueIncludes(IIncludeCompareExchangeValues values)
        {
            if (values.Results == null)
                return;

            throw new NotSupportedException();
        }

        public override Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> GetCompareExchangeValueIncludes()
        {
            throw new NotSupportedException();
        }

        public override Dictionary<string, Document>  GetRevisionIncludesByChangeVector()
        {
            throw new NotSupportedException();
        }

        public override Dictionary<string, Dictionary<DateTime, Document>> GetRevisionIncludesIdByDateTime()
        {
            throw new NotSupportedException();
        }

        public override void AddRevisionIncludes(IIncludeRevisions revisions)
        {
            if (revisions.RevisionsChangeVectorResults is null)
                return;
            
            throw new NotSupportedException();
        }

        public override bool SupportsExceptionHandling => false;
        public override bool SupportsInclude => false;
        public override bool SupportsHighlighting => false;
        public override bool SupportsExplanations => false;
    }
}
