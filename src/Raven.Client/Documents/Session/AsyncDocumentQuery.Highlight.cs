#if FEATURE_HIGHLIGHTING
using System;
using System.Collections;
using System.Linq.Expressions;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentQuery<T>
    {
        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight(
            string fieldName, int fragmentLength, int fragmentCount, string fragmentsField)
        {
            Highlight(fieldName, fragmentLength, fragmentCount, fragmentsField);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight(string fieldName, int fragmentLength, int fragmentCount,
            out FieldHighlightings fieldHighlightings)
        {
            Highlight(fieldName, fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight(string fieldName, string fieldKeyName, int fragmentLength, int fragmentCount,
            out FieldHighlightings fieldHighlightings)
        {
            Highlight(fieldName, fieldKeyName, fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight<TValue>(
            Expression<Func<T, TValue>> propertySelector,
            int fragmentLength,
            int fragmentCount,
            Expression<Func<T, IEnumerable>> fragmentsPropertySelector)
        {
            var fieldName = GetMemberQueryPath(propertySelector);
            var fragmentsField = GetMemberQueryPath(fragmentsPropertySelector);
            Highlight(fieldName, fragmentLength, fragmentCount, fragmentsField);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight<TValue>(
            Expression<Func<T, TValue>> propertySelector, int fragmentLength, int fragmentCount,
            out FieldHighlightings fieldHighlightings)
        {
            Highlight(GetMemberQueryPath(propertySelector), fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight<TValue>(
            Expression<Func<T, TValue>> propertySelector, Expression<Func<T, TValue>> keyPropertySelector, int fragmentLength, int fragmentCount,
            out FieldHighlightings fieldHighlightings)
        {
            Highlight(GetMemberQueryPath(propertySelector), GetMemberQueryPath(keyPropertySelector), fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.SetHighlighterTags(string preTag, string postTag)
        {
            SetHighlighterTags(new[] { preTag }, new[] { postTag });
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.SetHighlighterTags(string[] preTags, string[] postTags)
        {
            SetHighlighterTags(preTags, postTags);
            return this;
        }
    }
}
#endif
