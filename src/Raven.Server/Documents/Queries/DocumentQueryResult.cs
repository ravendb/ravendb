using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Explanation;

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

        public override void AddCounterIncludes(IncludeCountersCommand includeCountersCommand)
        {
            _counterIncludes = includeCountersCommand.Results;
            IncludedCounterNames = includeCountersCommand.CountersToGetByDocId;
        }

        public override Dictionary<string, List<CounterDetail>> GetCounterIncludes() => _counterIncludes;

        public override void AddResult(Document result)
        {
            Results.Add(result);
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

        public override void HandleException(Exception e)
        {
            throw new NotSupportedException();
        }

        private static string CreateExplanation(Lucene.Net.Search.Explanation explanation)
        {
            return explanation.ToString();
        }
    }

    public class IdsQueryResult : QueryResultServerSide<string>
    {
        public Queue<string> Ids { get; } = new Queue<string>();

        public override bool SupportsInclude => false;

        public override bool SupportsHighlighting => false;

        public override bool SupportsExplanations => false;

        public override bool SupportsExceptionHandling => false;

        public override void AddCounterIncludes(IncludeCountersCommand includeCountersCommand)
        {
            throw new NotSupportedException($"{nameof(IdsQueryResult)} doesn't support {nameof(AddCounterIncludes)}");
        }

        public override Dictionary<string, List<CounterDetail>> GetCounterIncludes() => throw new NotSupportedException($"{nameof(IdsQueryResult)} doesn't support {nameof(GetCounterIncludes)}");

        public override void AddResult(string result)
        {
            Ids.Enqueue(result);
        }

        public override void AddHighlightings(Dictionary<string, Dictionary<string, string[]>> highlightings)
        {
            throw new NotSupportedException($"{nameof(IdsQueryResult)} doesn't support {nameof(AddHighlightings)}");
        }

        public override void AddExplanation(ExplanationResult explanation)
        {
            throw new NotSupportedException($"{nameof(IdsQueryResult)} doesn't support {nameof(AddExplanation)}");
        }

        public override void HandleException(Exception e)
        {
            throw new NotSupportedException($"{nameof(IdsQueryResult)} doesn't support {nameof(HandleException)}");
        }
    }
}
