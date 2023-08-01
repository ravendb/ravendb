using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Explanation;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Sharding;

public sealed class ShardedQueryResult : QueryResultServerSide<BlittableJsonReaderObject>
{
    private IRevisionIncludes _includeRevisions;
    private ICounterIncludes _counterIncludes;
    private ITimeSeriesIncludes _includeTimeSeries;
    private ICompareExchangeValueIncludes _includeCompareExchangeValues;
    private List<IDisposable> _shardContextsToDispose;

    public ShardedQueryResult() : base(indexDefinitionRaftIndex: null)
    {
        
    }

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

    public override void AddCounterIncludes(ICounterIncludes counters)
    {
        _counterIncludes = counters;
        IncludedCounterNames = counters.IncludedCounterNames;
    }

    public override ICounterIncludes GetCounterIncludes()
    {
        return _counterIncludes;
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

    public void AddToDispose(IDisposable disposable)
    {
        _shardContextsToDispose ??= new List<IDisposable>();
        _shardContextsToDispose.Add(disposable);
    }

    public override void Dispose()
    {
        if (_shardContextsToDispose == null)
            return;

        foreach (var disposable in _shardContextsToDispose)
        {
            disposable.Dispose();
        }
    }
}
