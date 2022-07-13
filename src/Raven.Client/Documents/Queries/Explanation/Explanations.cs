using System.Collections.Generic;

namespace Raven.Client.Documents.Queries.Explanation
{
    public class Explanations
    {
        private Dictionary<string, string[]> _explanations;

        internal bool ShouldBeIncluded { get; set; } = false;

        public string[] GetExplanations(string key)
        {
            if (_explanations.TryGetValue(key, out var results) == false)
                return null;

            return results;
        }

        internal void Update(QueryResult queryResult)
        {
            _explanations = queryResult.Explanations;
        }
    }
}
