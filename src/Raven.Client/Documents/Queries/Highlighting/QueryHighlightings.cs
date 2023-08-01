using System.Collections.Generic;

namespace Raven.Client.Documents.Queries.Highlighting
{
    public sealed class QueryHighlightings
    {
        private readonly List<Highlightings> _highlightings = new List<Highlightings>();

        internal Highlightings Add(string fieldName)
        {
            var fieldHighlightings = new Highlightings(fieldName);
            _highlightings.Add(fieldHighlightings);
            return fieldHighlightings;
        }

        internal void Update(QueryResult queryResult)
        {
            foreach (var fieldHighlightings in _highlightings)
                fieldHighlightings.Update(queryResult.Highlightings);
        }
    }
}
