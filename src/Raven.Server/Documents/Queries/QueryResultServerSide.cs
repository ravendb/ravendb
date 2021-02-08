using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Explanation;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public abstract class QueryResultServerSide<T> : QueryResult<List<T>, List<T>>
    {
        protected QueryResultServerSide()
        {
            Results = new List<T>();
            Includes = new List<T>();
        }

        /// <summary>
        /// If the query returned time series results, this field will contain
        /// the names of the fields with time series data
        /// </summary>
        public List<string> TimeSeriesFields { get; set; }

        public void RegisterTimeSeriesFields(IndexQueryServerSide query, FieldsToFetch fields)
        {
            if (query.AddTimeSeriesNames == false || fields.AnyTimeSeries == false)
                return;

            foreach (var field in fields.Fields)
            {
                if (field.Value.IsTimeSeries)
                {
                    TimeSeriesFields ??= new List<string>();
                    if (fields.SingleBodyOrMethodWithNoAlias)
                    {
                        // in this case, we have an empty array, which indicate
                        // that we lifted the expression
                        return;
                    }

                    TimeSeriesFields.Add(field.Key);
                }
            }
        }

        public virtual void AddResult(T result)
        {
            Results.Add(result);
        }

        public abstract void AddHighlightings(Dictionary<string, Dictionary<string, string[]>> highlightings);

        public abstract void AddExplanation(ExplanationResult explanationResult);

        public abstract void HandleException(Exception e);

        public abstract bool SupportsExceptionHandling { get; }

        public abstract bool SupportsInclude { get; }

        public abstract bool SupportsHighlighting { get; }

        public abstract bool SupportsExplanations { get; }

        public bool NotModified { get; set; }

        public abstract void AddCounterIncludes(IncludeCountersCommand command);

        public abstract Dictionary<string, List<CounterDetail>> GetCounterIncludes();

        public abstract void AddTimeSeriesIncludes(IncludeTimeSeriesCommand command);

        public abstract Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>> GetTimeSeriesIncludes();

        public abstract void AddCompareExchangeValueIncludes(IncludeCompareExchangeValuesCommand command);

        public abstract Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> GetCompareExchangeValueIncludes();
    }

    public class ShardedQueryResult : QueryResultServerSide<BlittableJsonReaderObject>
    {
        public override void AddHighlightings(Dictionary<string, Dictionary<string, string[]>> highlightings)
        {
            throw new NotImplementedException();
        }

        public override void AddExplanation(ExplanationResult explanationResult)
        {
            throw new NotImplementedException();
        }

        public override void HandleException(Exception e)
        {
            throw new NotImplementedException();
        }

        public override bool SupportsExceptionHandling => true;
        public override bool SupportsInclude  => true;
        public override bool SupportsHighlighting  => true;
        public override bool SupportsExplanations  => true;
        public override void AddCounterIncludes(IncludeCountersCommand command)
        {
            throw new NotImplementedException();
        }

        public override Dictionary<string, List<CounterDetail>> GetCounterIncludes()
        {
            return null;
        }

        public override void AddTimeSeriesIncludes(IncludeTimeSeriesCommand command)
        {
            throw new NotImplementedException();
        }

        public override Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>> GetTimeSeriesIncludes()
        {
            return null;
        }

        public override void AddCompareExchangeValueIncludes(IncludeCompareExchangeValuesCommand command)
        {
            throw new NotImplementedException();
        }

        public override Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> GetCompareExchangeValueIncludes()
        {
            return null;
        }
    }
}
