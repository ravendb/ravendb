//-----------------------------------------------------------------------
// <copyright file="RavenQueryInspector.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Highlighting;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Linq
{
    /// <summary>
    /// Implements <see cref="IRavenQueryable{T}"/>
    /// </summary>
    public class RavenQueryInspector<T> : IRavenQueryable<T>, IRavenQueryInspector
    {
        private Expression _expression;
        private IRavenQueryProvider _provider;
        private QueryStatistics _queryStats;
        private LinqQueryHighlightings _highlightings;
        private string _indexName;
        private string _collectionName;
        private InMemoryDocumentSessionOperations _session;
        private bool _isMapReduce;
        private DocumentConventions _conventions;
        private bool _isAsync;

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenQueryInspector{T}"/> class.
        /// </summary>
        public void Init(
            IRavenQueryProvider provider,
            QueryStatistics queryStats,
            LinqQueryHighlightings highlightings,
            string indexName,
            string collectionName,
            Expression expression,
            InMemoryDocumentSessionOperations session,
            bool isMapReduce)
        {
            _conventions = session.Conventions;
            _provider = provider?.For<T>() ?? throw new ArgumentNullException(nameof(provider));
            _queryStats = queryStats;
            _highlightings = highlightings;
            _indexName = indexName;
            _collectionName = collectionName;
            _session = session;
            _isAsync = session is AsyncDocumentSession;
            _isMapReduce = isMapReduce;
            _provider.AfterQueryExecuted(AfterQueryExecuted);
            _expression = expression ?? Expression.Constant(this);
        }

        private void AfterQueryExecuted(QueryResult queryResult)
        {
            _queryStats.UpdateQueryStats(queryResult);
            _highlightings.Update(queryResult);
        }

        #region IOrderedQueryable<T> Members

        Expression IQueryable.Expression => _expression;

        Type IQueryable.ElementType => typeof(T);

        IQueryProvider IQueryable.Provider => _provider;

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<T> GetEnumerator()
        {
            var execute = _provider.Execute(_expression);
            return ((IEnumerable<T>)execute).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        /// <summary>
        /// Provide statistics about the query, such as total count of matching records
        /// </summary>
        public IRavenQueryable<T> Statistics(out QueryStatistics stats)
        {
            stats = _queryStats;
            return this;
        }

        /// <summary>
        /// Customizes the query using the specified action
        /// </summary>
        /// <param name="action">The action.</param>
        /// <returns></returns>
        public IRavenQueryable<T> Customize(Action<IDocumentQueryCustomization> action)
        {
            _provider.Customize(action);
            return this;
        }

        public IRavenQueryable<T> Highlight(string fieldName, int fragmentLength, int fragmentCount, out Highlightings highlightings)
        {
            return Highlight(fieldName, fragmentLength, fragmentCount, null, out highlightings);
        }

        public IRavenQueryable<T> Highlight(string fieldName, int fragmentLength, int fragmentCount, HighlightingOptions options,
            out Highlightings highlightings)
        {
            highlightings = _highlightings.Add(fieldName, fragmentLength, fragmentCount, options);
            return this;
        }

        public IRavenQueryable<T> Highlight(Expression<Func<T, object>> path, int fragmentLength, int fragmentCount, out Highlightings highlightings)
        {
            return Highlight(path.ToPropertyPath(), fragmentLength, fragmentCount, out highlightings);
        }

        public IRavenQueryable<T> Highlight(Expression<Func<T, object>> path, int fragmentLength, int fragmentCount, HighlightingOptions options,
            out Highlightings highlightings)
        {
            return Highlight(path.ToPropertyPath(), fragmentLength, fragmentCount, options, out highlightings);
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            if (_isAsync)
            {
                var asyncDocumentQuery = GetAsyncDocumentQuery();
                return asyncDocumentQuery.ToString();
            }

            var documentQuery = GetDocumentQuery();
            return documentQuery.ToString();
        }

        public IndexQuery GetIndexQuery(bool isAsync = true)
        {
            if (isAsync == false)
            {
                var documentQuery = GetDocumentQuery();
                return documentQuery.GetIndexQuery();
            }

            var asyncDocumentQuery = GetAsyncDocumentQuery();
            return asyncDocumentQuery.GetIndexQuery();
        }

        internal IDocumentQuery<T> GetDocumentQuery(Action<IAbstractDocumentQuery<T>> customization = null)
        {
            if (_isAsync)
                throw new InvalidOperationException("Cannot convert async query to sync document query.");

            var ravenQueryProvider = GetRavenQueryProvider();
            return ravenQueryProvider.GetDocumentQueryFor(_expression, customization);
        }

        internal IAsyncDocumentQuery<T> GetAsyncDocumentQuery(Action<IAbstractDocumentQuery<T>> customization = null)
        {
            if (_isAsync == false)
                throw new InvalidOperationException("Cannot convert sync query to async document query.");

            var ravenQueryProvider = GetRavenQueryProvider();
            return ravenQueryProvider.GetAsyncDocumentQueryFor(_expression, customization);
        }

        private RavenQueryProviderProcessor<T> GetRavenQueryProvider()
        {
            return new RavenQueryProviderProcessor<T>(
                _provider.QueryGenerator,
                _provider.CustomizeQuery,
                null,
                _highlightings,
                _indexName,
                _collectionName,
                new HashSet<FieldToFetch>(_provider.FieldsToFetch),
                _isMapReduce,
                _provider.OriginalQueryType,
                _conventions);
        }

        public string IndexName => _indexName;

        public InMemoryDocumentSessionOperations Session => _session;

        /// <summary>
        /// Set the fields to fetch
        /// </summary>
        public void FieldsToFetch(IEnumerable<string> fields)
        {
            foreach (var field in fields)
            {
                _provider.FieldsToFetch.Add(new FieldToFetch(field, null));
            }
        }
    }
}
