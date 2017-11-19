#if FEATURE_HIGHLIGHTING
using System;
using System.Collections.Generic;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///   A query against a Raven index
    /// </summary>
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        /// <summary>
        ///   The fields to highlight
        /// </summary>
        protected List<HighlightedField> HighlightedFields = new List<HighlightedField>();

        /// <summary>
        ///   Highlighter pre tags
        /// </summary>
        protected string[] HighlighterPreTags = new string[0];

        /// <summary>
        ///   Highlighter post tags
        /// </summary>
        protected string[] HighlighterPostTags = new string[0];

        /// <summary>
        ///   Highlighter key
        /// </summary>
        protected string HighlighterKeyName;

        /// <summary>
        /// Holds the query highlights
        /// </summary>
        protected QueryHighlightings Highlightings = new QueryHighlightings();

        /// <inheritdoc />
        public void SetHighlighterTags(string preTag, string postTag)
        {
            SetHighlighterTags(new[] { preTag }, new[] { postTag });
        }

        /// <inheritdoc />
        public void Highlight(string fieldName, int fragmentLength, int fragmentCount, string fragmentsField)
        {
            throw new NotImplementedException("This feature is not yet implemented");
            //HighlightedFields.Add(new HighlightedField(fieldName, fragmentLength, fragmentCount, fragmentsField));
        }

        /// <inheritdoc />
        public void Highlight(string fieldName, int fragmentLength, int fragmentCount, out FieldHighlightings fieldHighlightings)
        {
            throw new NotImplementedException("This feature is not yet implemented");
            //HighlightedFields.Add(new HighlightedField(fieldName, fragmentLength, fragmentCount, null));
            //fieldHighlightings = Highlightings.AddField(fieldName);
        }

        /// <inheritdoc />
        public void Highlight(string fieldName, string fieldKeyName, int fragmentLength, int fragmentCount, out FieldHighlightings fieldHighlightings)
        {
            throw new NotImplementedException("This feature is not yet implemented");
            //HighlighterKeyName = fieldKeyName;
            //HighlightedFields.Add(new HighlightedField(fieldName, fragmentLength, fragmentCount, null));
            //fieldHighlightings = Highlightings.AddField(fieldName);
        }

        /// <inheritdoc />
        public void SetHighlighterTags(string[] preTags, string[] postTags)
        {
            throw new NotImplementedException("This feature is not yet implemented");
            //HighlighterPreTags = preTags;
            //HighlighterPostTags = postTags;
        }
    }
}
#endif
