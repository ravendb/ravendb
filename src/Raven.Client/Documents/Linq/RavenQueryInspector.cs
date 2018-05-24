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
using Raven.Client.Documents.Session;

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
#if FEATURE_HIGHLIGHTING
        private QueryHighlightings _highlightings;
#endif
        private string _indexName;
        private string _collectionName;
        private InMemoryDocumentSessionOperations _session;
        private bool _isMapReduce;
        private DocumentConventions _conventions;

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenQueryInspector{T}"/> class.
        /// </summary>
        public void Init(
            IRavenQueryProvider provider,
            QueryStatistics queryStats,
#if FEATURE_HIGHLIGHTING
            QueryHighlightings highlightings,
#endif
            string indexName,
            string collectionName,
            Expression expression,
            InMemoryDocumentSessionOperations session,
            bool isMapReduce)
        {
            _conventions = session.Conventions;
            _provider = provider?.For<T>() ?? throw new ArgumentNullException(nameof(provider));
            _queryStats = queryStats;
#if FEATURE_HIGHLIGHTING
            _highlightings = highlightings;
#endif
            _indexName = indexName;
            _collectionName = collectionName;
            _session = session;
            _isMapReduce = isMapReduce;
            _provider.AfterQueryExecuted(AfterQueryExecuted);
            _expression = expression ?? Expression.Constant(this);
        }

        private void AfterQueryExecuted(QueryResult queryResult)
        {
            _queryStats.UpdateQueryStats(queryResult);
#if FEATURE_HIGHLIGHTING
            _highlightings.Update(queryResult);
#endif
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

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            RavenQueryProviderProcessor<T> ravenQueryProvider = GetRavenQueryProvider();
            string query;
            if (_session is AsyncDocumentSession)
            {
                var asyncDocumentQuery = ravenQueryProvider.GetAsyncDocumentQueryFor(_expression);
                query = asyncDocumentQuery.GetIndexQuery().ToString();
            }
            else
            {
                var documentQuery = ravenQueryProvider.GetDocumentQueryFor(_expression);
                query = documentQuery.ToString();
            }

            return query;
        }

        public IndexQuery GetIndexQuery(bool isAsync = true)
        {
            RavenQueryProviderProcessor<T> ravenQueryProvider = GetRavenQueryProvider();
            if (isAsync == false)
            {
                var documentQuery = ravenQueryProvider.GetDocumentQueryFor(_expression);
                return documentQuery.GetIndexQuery();
            }
            var asyncDocumentQuery = ravenQueryProvider.GetAsyncDocumentQueryFor(_expression);
            return asyncDocumentQuery.GetIndexQuery();
        }

        private RavenQueryProviderProcessor<T> GetRavenQueryProvider()
        {
            return new RavenQueryProviderProcessor<T>(
                _provider.QueryGenerator,
                _provider.CustomizeQuery,
                null,
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
