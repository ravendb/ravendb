using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Explanation;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class DocumentQueryResult : QueryResultServerSide<Document>
    {
        public static readonly DocumentQueryResult NotModifiedResult = new DocumentQueryResult { NotModified = true };

        public override bool SupportsInclude => true;

        public override bool SupportsHighlighting => true;

        public override bool SupportsExplanations => true;

        public override bool SupportsExceptionHandling => false;

        private Dictionary<string, List<CounterDetail>> _counterIncludes;

        private Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>> _timeSeriesIncludes;

        private Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> _compareExchangeValueIncludes;

        public override void AddCounterIncludes(IncludeCountersCommand includeCountersCommand)
        {
            _counterIncludes = includeCountersCommand.Results;
            IncludedCounterNames = includeCountersCommand.CountersToGetByDocId;
        }

        public override Dictionary<string, List<CounterDetail>> GetCounterIncludes() => _counterIncludes;
        public override void AddTimeSeriesIncludes(IncludeTimeSeriesCommand includeTimeSeriesCommand)
        {
            _timeSeriesIncludes = includeTimeSeriesCommand.Results;
        }

        public override Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>> GetTimeSeriesIncludes() => _timeSeriesIncludes;

        public override void AddCompareExchangeValueIncludes(IncludeCompareExchangeValuesCommand command)
        {
            _compareExchangeValueIncludes = command.Results;
        }

        public override Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> GetCompareExchangeValueIncludes() => _compareExchangeValueIncludes;


        public override void AddHighlightings(Dictionary<string, Dictionary<string, string[]>> highlightings)
        {
            if (Highlightings == null)
                Highlightings = new Dictionary<string, Dictionary<string, string[]>>();

            foreach (var kvp in highlightings)
            {
                if (Highlightings.TryGetValue(kvp.Key, out var result) == false)
                    Highlightings[kvp.Key] = result = new Dictionary<string, string[]>();

                foreach (var innerKvp in kvp.Value)
                {
                    if (result.TryGetValue(innerKvp.Key, out var innerResult))
                    {
                        Array.Resize(ref innerResult, innerResult.Length + innerKvp.Value.Length);
                        Array.Copy(innerKvp.Value, 0, innerResult, innerResult.Length, innerKvp.Value.Length);
                    }
                    else
                        result[innerKvp.Key] = innerKvp.Value;
                }
            }
        }

        public override void AddExplanation(ExplanationResult explanation)
        {
            if (Explanations == null)
                Explanations = new Dictionary<string, string[]>();

            if (Explanations.TryGetValue(explanation.Key, out var result) == false)
                Explanations[explanation.Key] = new[] { CreateExplanation(explanation.Explanation) };
            else
            {
                Array.Resize(ref result, result.Length + 1);
                result[result.Length - 1] = CreateExplanation(explanation.Explanation);
            }
        }

        public override void HandleException(Exception e)
        {
            throw new NotSupportedException();
        }

        private static string CreateExplanation(Lucene.Net.Search.Explanation explanation)
        {
            return explanation.ToString();
        }
    }
}
