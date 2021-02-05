using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Spatial;
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

        /// <summary>
        /// If the query returned spatial properties results, this field will contain
        /// the list of longitude & latitude document properties names from the spatial query
        /// </summary>
        public SpatialProperty[] SpatialProperties { get; set; }

        /// <summary>
        /// If the query returned spatial shapes results,
        /// this field will contain the shapes info from the spatial query
        /// </summary>
        public SpatialShapeBase[] SpatialShapes { get; set; }

        public void RegisterSpatialProperties(IndexQueryServerSide query)
        {
            if (query.AddSpatialProperties == false)
                return;

            if (query.Metadata.SpatialProperties != null)
                SpatialProperties = query.Metadata.SpatialProperties.ToArray();

            if (query.Metadata.SpatialShapes != null)
                SpatialShapes = query.Metadata.SpatialShapes.ToArray();
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

        public abstract void AddCounterIncludes(IncludeCountersCommand command);

        public abstract Dictionary<string, List<CounterDetail>> GetCounterIncludes();

        public abstract void AddTimeSeriesIncludes(IncludeTimeSeriesCommand command);

        public abstract Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>> GetTimeSeriesIncludes();

        public abstract void AddCompareExchangeValueIncludes(IncludeCompareExchangeValuesCommand command);

        public abstract Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> GetCompareExchangeValueIncludes();
    }
}
