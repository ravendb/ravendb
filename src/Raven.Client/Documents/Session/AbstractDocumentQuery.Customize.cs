using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///   A query against a Raven index
    /// </summary>
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        protected Action<IndexQuery> BeforeQueryExecutionAction;

        public QueryOperation QueryOperation { get; protected set; }

        /// <inheritdoc />
        public IDocumentQueryCustomization BeforeQueryExecution(Action<IndexQuery> action)
        {
            BeforeQueryExecutionAction += action;
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.Highlight(string fieldName, int fragmentLength, int fragmentCount, string fragmentsField)
        {
            Highlight(fieldName, fragmentLength, fragmentCount, fragmentsField);
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.Highlight(string fieldName, int fragmentLength, int fragmentCount, out FieldHighlightings fieldHighlightings)
        {
            Highlight(fieldName, fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.Highlight(string fieldName, string fieldKeyName, int fragmentLength, int fragmentCount, out FieldHighlightings fieldHighlightings)
        {
            Highlight(fieldName, fieldKeyName, fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.SetHighlighterTags(string preTag, string postTag)
        {
            SetHighlighterTags(preTag, postTag);
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.SetHighlighterTags(string[] preTags, string[] postTags)
        {
            SetHighlighterTags(preTags, postTags);
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.Include<TResult>(Expression<Func<TResult, object>> path)
        {
            if (path.Body is UnaryExpression body)
            {
                switch (body.NodeType)
                {
                    case ExpressionType.Convert:
                    case ExpressionType.ConvertChecked:
                        throw new InvalidOperationException("You cannot use Include<TResult> on value type. Please use the Include<TResult, TInclude> overload.");
                }
            }

            Include(path.ToPropertyPath());
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.Include<TResult, TInclude>(Expression<Func<TResult, object>> path)
        {
            var idPrefix = Conventions.GetCollectionName(typeof(TInclude));
            if (idPrefix != null)
            {
                idPrefix = Conventions.TransformTypeCollectionNameToDocumentIdPrefix(idPrefix);
                idPrefix += Conventions.IdentityPartsSeparator;
            }

            var id = path.ToPropertyPath() + "(" + idPrefix + ")";
            Include(id);
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.Include(string path)
        {
            Include(path);
            return this;
        }

        /// <inheritdoc />
        public IDocumentQueryCustomization NoTracking()
        {
            DisableEntitiesTracking = true;
            return this;
        }

        /// <inheritdoc />
        public IDocumentQueryCustomization NoCaching()
        {
            DisableCaching = true;
            return this;
        }

        /// <inheritdoc />
        public IDocumentQueryCustomization ShowTimings()
        {
            ShowQueryTimings = true;
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.AddOrder(string fieldName, bool descending, OrderingType ordering)
        {
            if (descending)
                OrderByDescending(fieldName, ordering);
            else
                OrderBy(fieldName, ordering);

            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.AddOrder<TResult>(Expression<Func<TResult, object>> propertySelector, bool descending, OrderingType ordering)
        {
            var fieldName = GetMemberQueryPath(propertySelector.Body);
            if (descending)
                OrderByDescending(fieldName, ordering);
            else
                OrderBy(fieldName, ordering);

            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.RandomOrdering()
        {
            RandomOrdering();
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.RandomOrdering(string seed)
        {
            RandomOrdering(seed);
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.CustomSortUsing(string typeName)
        {
            CustomSortUsing(typeName, false);
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.CustomSortUsing(string typeName, bool descending)
        {
            CustomSortUsing(typeName, descending);
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResults(TimeSpan waitTimeout)
        {
            WaitForNonStaleResults(waitTimeout);
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResults()
        {
            WaitForNonStaleResults();
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
        {
            WaitForNonStaleResultsAsOfNow(waitTimeout);
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOf(long cutOffEtag)
        {
            WaitForNonStaleResultsAsOf(cutOffEtag);
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOf(long cutOffEtag, TimeSpan waitTimeout)
        {
            WaitForNonStaleResultsAsOf(cutOffEtag, waitTimeout);
            return this;
        }

        /// <inheritdoc />
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOfNow()
        {
            WaitForNonStaleResultsAsOfNow();
            return this;
        }
    }
}
