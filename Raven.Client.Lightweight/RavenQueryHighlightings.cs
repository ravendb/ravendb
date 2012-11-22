using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Client
{
    /// <summary>
    ///     Query highlightings for the documents.
    /// </summary>
    public class RavenQueryHighlightings
    {
        private readonly Dictionary<string, Dictionary<string, string[]>> highlightings;

        public RavenQueryHighlightings()
        {
            this.highlightings = new Dictionary<string, Dictionary<string, string[]>>();
        }

        /// <summary>
        ///     Returns the list of document's field highlighting fragments.
        /// </summary>
        /// <param name="documentId">The document id.</param>
        /// <param name="fieldName">The document's field name.</param>
        /// <returns></returns>
        public string[] GetFragments(string documentId, string fieldName)
        {
            Dictionary<string, string[]> documentFragments;

            if (!this.highlightings.TryGetValue(documentId, out documentFragments))
                return new string[0];

            string[] result;

            if (!documentFragments.TryGetValue(fieldName, out result))
                return new string[0];

            return result;
        }

        internal void Update(QueryResult queryResult)
        {
            this.highlightings.Clear();

            foreach (var fragment in queryResult.Highlightings)
                this.highlightings.Add(fragment.Key, fragment.Value);
        }
    }
}