using System;
using System.Collections.Generic;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Query highlights for the documents.
    /// </summary>
    public class FieldHighlightings
    {
        private readonly Dictionary<string,string[]> _highlightings;

        public FieldHighlightings(string fieldName)
        {
            FieldName = fieldName;
            _highlightings = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        ///     The field name.
        /// </summary>
        public string FieldName { get; }

        public IEnumerable<string> ResultIndents => _highlightings.Keys;

        /// <summary>
        ///     Returns the list of document's field highlighting fragments.
        /// </summary>
        /// <param name="key">The document id, or the map/reduce key field.</param>
        /// <returns></returns>
        public string[] GetFragments(string key)
        {
            string[] result;

            if (!_highlightings.TryGetValue(key, out result))
                return new string[0];

            return result;
        }

        internal void Update(QueryResult queryResult)
        {
            _highlightings.Clear();

            if (queryResult.Highlightings == null)
                return;

            foreach (var entityFragments in queryResult.Highlightings)
                foreach (var fieldFragments in entityFragments.Value)
                    if (fieldFragments.Key == FieldName)
                        _highlightings.Add(entityFragments.Key, fieldFragments.Value);
        }
    }
}
