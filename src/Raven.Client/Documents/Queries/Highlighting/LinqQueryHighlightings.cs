using System.Collections.Generic;

namespace Raven.Client.Documents.Queries.Highlighting
{
    public class LinqQueryHighlightings
    {
        private readonly List<Highlightings> _highlightings = new List<Highlightings>();

        internal readonly List<(string FieldName, int FragmentLength, int FragmentCount, HighlightingOptions Options)> Highlightings = new List<(string FieldName, int FragmentLength, int FragmentCount, HighlightingOptions Options)>();

        internal Highlightings Add(string fieldName, int fragmentLength, int fragmentCount, HighlightingOptions options)
        {
            Highlightings.Add((fieldName, fragmentLength, fragmentCount, options));

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
