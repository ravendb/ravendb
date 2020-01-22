using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Explanation;

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

        public void RegisterTimeSeriesFields(FieldsToFetch fields)
        {
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
                    TimeSeriesFields .Add(field.Key);
                }
            }
        }

        public abstract void AddResult(T result);

        public abstract void AddHighlightings(Dictionary<string, Dictionary<string, string[]>> highlightings);

        public abstract void AddExplanation(ExplanationResult explanationResult);

        public abstract void HandleException(Exception e);

        public abstract bool SupportsExceptionHandling { get; }

        public abstract bool SupportsInclude { get; }

        public abstract bool SupportsHighlighting { get; }

        public abstract bool SupportsExplanations { get; }

        public bool NotModified { get; set; }

        public abstract void AddCounterIncludes(IncludeCountersCommand includeCountersCommand);

        public abstract Dictionary<string, List<CounterDetail>> GetCounterIncludes();

        public abstract void AddTimeSeriesIncludes(IncludeTimeSeriesCommand includeTimeSeriesCommand);

        public abstract Dictionary<string, List<TimeSeriesRangeResult>> GetTimeSeriesIncludes();


    }
}
