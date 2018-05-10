using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Queries
{
    public class DocumentQueryResult : QueryResultServerSide
    {
        public static readonly DocumentQueryResult NotModifiedResult = new DocumentQueryResult { NotModified = true };

        public override bool SupportsInclude => true;

        public override bool SupportsHighlighting => true;

        public override bool SupportsExceptionHandling => false;

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

        public override void HandleException(Exception e)
        {
            throw new NotSupportedException();
        }
    }
}
