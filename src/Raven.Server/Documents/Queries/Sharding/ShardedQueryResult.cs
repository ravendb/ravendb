using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Explanation;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Sharding;

public class ShardedQueryResult : QueryResultServerSide<BlittableJsonReaderObject>
{
    private ICompareExchangeValueIncludes _includeCompareExchangeValues;
    private IRevisionIncludes _includeRevisions;
    private ITimeSeriesIncludes _includeTimeSeries;
    public override ValueTask AddResultAsync(BlittableJsonReaderObject result, CancellationToken token)
    {
        throw new NotSupportedException();
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
        throw new NotSupportedException();
    }

    public override bool SupportsExceptionHandling => true;
    public override bool SupportsInclude  => true;
    public override bool SupportsHighlighting  => true;
    public override bool SupportsExplanations  => true;

    public override void AddCounterIncludes(IncludeCountersCommand command)
    {
        throw new NotSupportedException();
    }

    public override Dictionary<string, List<CounterDetail>> GetCounterIncludes()
    {
        return null;
    }

    public override void AddTimeSeriesIncludes(ITimeSeriesIncludes timeSeries)
    {
        _includeTimeSeries = timeSeries;
    }

    public override ITimeSeriesIncludes GetTimeSeriesIncludes()
    {
        return _includeTimeSeries;
    }

    public override void AddCompareExchangeValueIncludes(ICompareExchangeValueIncludes values)
    {
        _includeCompareExchangeValues = values;
    }

    public override Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> GetCompareExchangeValueIncludes() => _includeCompareExchangeValues?.Results;

    public override void AddRevisionIncludes(IRevisionIncludes revisions)
    {
        _includeRevisions = revisions;
    }

    public override IRevisionIncludes GetRevisionIncludes()
    {
        return _includeRevisions;
    }
}
