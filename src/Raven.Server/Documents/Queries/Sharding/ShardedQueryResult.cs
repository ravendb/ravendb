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

namespace Raven.Server.Documents.Queries.Sharding;

public class ShardedQueryResult : QueryResultServerSide<BlittableJsonReaderObject>
{
    private IncludeCompareExchangeValuesCommand _includeCompareExchangeValues;

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

    public override void AddTimeSeriesIncludes(IncludeTimeSeriesCommand command)
    {
        throw new NotSupportedException();
    }

    public override Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>> GetTimeSeriesIncludes()
    {
        return null;
    }

    public override void AddCompareExchangeValueIncludes(IncludeCompareExchangeValuesCommand command)
    {
        _includeCompareExchangeValues = command;
    }

    public override Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> GetCompareExchangeValueIncludes() => _includeCompareExchangeValues?.Results;

    public override void AddRevisionIncludes(IncludeRevisionsCommand command)
    {
        throw new NotSupportedException();
    }

    public override Dictionary<string, Document> GetRevisionIncludesByChangeVector()
    {
        return null;
    }

    public override Dictionary<string, Dictionary<DateTime, Document>> GetRevisionIncludesIdByDateTime()
    {
        return null;
    }
}
