using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Queries.Highlighting
{
    /// <summary>
    ///     Query highlightings for the documents.
    /// </summary>
    public sealed class Highlightings
    {
        private readonly Dictionary<string, string[]> _highlightings;

        public Highlightings(string fieldName)
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
            if (_highlightings.TryGetValue(key, out var result) == false)
                return new string[0];

            return result;
        }

        internal void Update(Dictionary<string, Dictionary<string, string[]>> highlightings)
        {
            _highlightings.Clear();

            if (highlightings == null || highlightings.TryGetValue(FieldName, out var result) == false)
                return;

            foreach (var kvp in result)
                _highlightings.Add(kvp.Key, kvp.Value);
        }
    }
}
