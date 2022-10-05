using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client.Documents.Queries.Facets;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes.Persistence;

public abstract class IndexFacetReadOperationBase : IndexOperationBase
{
    protected readonly QueryBuilderFactories _queryBuilderFactories;

    public IndexFacetReadOperationBase(Index index, QueryBuilderFactories queryBuilderFactories, Logger logger) : base(index, logger)
    {
        _queryBuilderFactories = queryBuilderFactories;
    }

    public abstract List<FacetResult> FacetedQuery(FacetQuery facetQuery, QueryTimingsScope queryTimings, DocumentsOperationContext context,
        Func<string, SpatialField> getSpatialField, CancellationToken token);

    protected static void CompleteFacetCalculationsStage(Dictionary<string, FacetedQueryParser.FacetResult> results)
    {
        foreach (var result in results)
        {
            foreach (var value in result.Value.Result.Values)
            {
                if (value.Average.HasValue == false)
                    continue;

                if (value.Count == 0)
                    value.Average = double.NaN;
                else
                    value.Average = value.Average / value.Count;
            }
        }
    }
    
    protected class FacetValues
    {
        private static readonly FacetAggregationField Default = new FacetAggregationField();

        private readonly bool _legacy;
        private readonly Dictionary<FacetAggregationField, FacetValue> _values = new Dictionary<FacetAggregationField, FacetValue>();

        public int Count;
        public bool Any;

        public FacetValues(bool legacy)
        {
            _legacy = legacy;
        }

        public void AddDefault(string range)
        {
            Any = true;
            _values[Default] = new FacetValue {Range = range};
        }

        public void Add(FacetAggregationField field, string range)
        {
            if (_legacy)
            {
                if (Any)
                    return;

                AddDefault(range);
                return;
            }

            Any = true;
            _values[field] = new FacetValue {Range = range, Name = string.IsNullOrWhiteSpace(field.DisplayName) ? field.Name : field.DisplayName};
        }

        public FacetValue Get(FacetAggregationField field)
        {
            if (_legacy)
                return _values[Default];

            return _values[field];
        }

        public IEnumerable<FacetValue> GetAll()
        {
            return _values.Values;
        }

        public void IncrementCount(int count)
        {
            Count += count;

            foreach (var facetValue in _values)
                facetValue.Value.Count += count;
        }
    }

    public override void Dispose()
    {
    }
}
