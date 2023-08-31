using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Explanation;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class DocumentQueryResult : QueryResultServerSide<Document>
    {
        public static readonly DocumentQueryResult NotModifiedResult = new DocumentQueryResult(null) { NotModified = true };

        public override bool SupportsInclude => true;

        public override bool SupportsHighlighting => true;

        public override bool SupportsExplanations => true;

        public override bool SupportsExceptionHandling => false;

        private ICounterIncludes _counterIncludes;

        private IRevisionIncludes _revisionIncludes;

        private ITimeSeriesIncludes _timeSeriesIncludes;

        private Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> _compareExchangeValueIncludes;

        public DocumentQueryResult(long? indexDefinitionRaftIndex) : base(indexDefinitionRaftIndex)
        {
        }

        public override void AddCounterIncludes(ICounterIncludes includeCountersCommand)
        {
            _counterIncludes = includeCountersCommand;
            IncludedCounterNames = includeCountersCommand.IncludedCounterNames;
        }

        public override ICounterIncludes GetCounterIncludes() => _counterIncludes;

        public override void AddTimeSeriesIncludes(ITimeSeriesIncludes includeTimeSeriesCommand)
        {
            _timeSeriesIncludes = includeTimeSeriesCommand;
        }

        public override ITimeSeriesIncludes GetTimeSeriesIncludes() => _timeSeriesIncludes;

        public override void AddCompareExchangeValueIncludes(ICompareExchangeValueIncludes values)
        {
            _compareExchangeValueIncludes = values.Results;
        }

        public override Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> GetCompareExchangeValueIncludes() => _compareExchangeValueIncludes;
        
        public override void AddRevisionIncludes(IRevisionIncludes revisions)
        {
            _revisionIncludes = revisions;
        }

        public override IRevisionIncludes GetRevisionIncludes() => _revisionIncludes;

        public override ValueTask AddResultAsync(Document result, CancellationToken token)
        {
            Results.Add(result);
            return default;
        }

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

        public override ValueTask HandleExceptionAsync(Exception e, CancellationToken token)
        {
            throw new NotSupportedException();
        }

        private static string CreateExplanation(Lucene.Net.Search.Explanation explanation)
        {
            return explanation.ToString();
        }
    }
}
