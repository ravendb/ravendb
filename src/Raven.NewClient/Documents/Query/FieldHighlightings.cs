using System;
using System.Collections.Generic;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Documents;

namespace Raven.NewClient.Client.Documents
{
    /// <summary>
    ///     Query highlightings for the documents.
    /// </summary>
    public class FieldHighlightings
    {
        private readonly Dictionary<string,string[]> highlightings;

        public FieldHighlightings(string fieldName)
        {
            this.FieldName = fieldName;
            this.highlightings = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        ///     The field name.
        /// </summary>
        public string FieldName { get; private set; }

        public IEnumerable<string> ResultIndents
        {
            get { return this.highlightings.Keys; }
        }

        /// <summary>
        ///     Returns the list of document's field highlighting fragments.
        /// </summary>
        /// <param name="key">The document id, or the map/reduce key field.</param>
        /// <returns></returns>
        public string[] GetFragments(string key)
        {
            string[] result;

            if (!this.highlightings.TryGetValue(key, out result))
                return new string[0];

            return result;
        }

        internal void Update(QueryResult queryResult)
        {
            this.highlightings.Clear();

            if (queryResult.Highlightings == null)
                return;

            foreach (var entityFragments in queryResult.Highlightings)
                foreach (var fieldFragments in entityFragments.Value)
                    if (fieldFragments.Key == this.FieldName)
                        this.highlightings.Add(entityFragments.Key, fieldFragments.Value);
        }
    }
}
