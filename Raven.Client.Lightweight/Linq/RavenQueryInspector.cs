using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Client;
using Raven.Database.Data;

namespace Raven.Client.Linq
{
	/// <summary>
	/// Implements <see cref="IRavenQueryable{T}"/>
	/// </summary>
    public class RavenQueryInspector<T> : IRavenQueryable<T>, IRavenQueryInspector
    {
        private readonly Expression expression;
        private readonly IRavenQueryProvider provider;
	    private readonly RavenQueryStatistics queryStats;

	    /// <summary>
	    /// Initializes a new instance of the <see cref="RavenQueryInspector{T}"/> class.
	    /// </summary>
	    /// <param name="provider">The provider.</param>
	    /// <param name="queryStats">The query stats to fill</param>
	    public RavenQueryInspector(IRavenQueryProvider provider, RavenQueryStatistics queryStats)
        {
            if (provider == null)
            {
                throw new ArgumentNullException("provider");
            }
            this.provider = provider;
	        this.queryStats = queryStats;
	        this.provider.AfterQueryExecuted(UpdateQueryStats);
            expression = Expression.Constant(this);
        }

	    /// <summary>
		/// Initializes a new instance of the <see cref="RavenQueryInspector{T}"/> class.
		/// </summary>
		/// <param name="provider">The provider.</param>
		/// <param name="expression">The expression.</param>
        /// <param name="queryStats">The query stats to fill</param>
        public RavenQueryInspector(IRavenQueryProvider provider, Expression expression, RavenQueryStatistics queryStats)
        {
            if (provider == null)
            {
                throw new ArgumentNullException("provider");
            }
            if (expression == null)
            {
                throw new ArgumentNullException("expression");
            }
		    this.provider = provider.For<T>();
            this.provider.AfterQueryExecuted(UpdateQueryStats);
            this.expression = expression;
	        this.queryStats = queryStats;
        }

        private void UpdateQueryStats(QueryResult obj)
        {
            queryStats.IsStale = obj.IsStale;
            queryStats.TotalResults = obj.TotalResults;
            queryStats.SkippedResults = obj.SkippedResults;
            queryStats.Timestamp = obj.IndexTimestamp;
        }

        #region IOrderedQueryable<T> Members

        Expression IQueryable.Expression
        {
            get { return expression; }
        }

        Type IQueryable.ElementType
        {
            get { return typeof(T); }
        }

        IQueryProvider IQueryable.Provider
        {
            get { return provider; }
        }

		/// <summary>
		/// Gets the enumerator.
		/// </summary>
		/// <returns></returns>
        public IEnumerator<T> GetEnumerator()
        {
            return ((IEnumerable<T>)provider.Execute(expression)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        /// <summary>
        /// Provide statistics about the query, such as total count of matching records
        /// </summary>
	    public IRavenQueryable<T> Statistics(out RavenQueryStatistics stats)
	    {
	        stats = queryStats;
	        return this;
	    }

	    /// <summary>
		/// Customizes the query using the specified action
		/// </summary>
		/// <param name="action">The action.</param>
		/// <returns></returns>
        public IRavenQueryable<T> Customize(Action<IDocumentQueryCustomization> action)
        {
            provider.Customize(action);
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
            var ravenQueryProvider = new RavenQueryProviderProcessor<T>(provider.Session, null, null, provider.IndexName);
            ravenQueryProvider.ProcessExpression(expression);
            string fields = "";
            if (ravenQueryProvider.FieldsToFetch.Count > 0)
                fields = "<" + string.Join(", ", ravenQueryProvider.FieldsToFetch.ToArray()) + ">: ";
            return 
                fields + 
                ravenQueryProvider.LuceneQuery;
        }

        /// <summary>
        /// Get the name of the index being queried
        /// </summary>
	    public string IndexQueried
	    {
	        get
	        {
                var ravenQueryProvider = new RavenQueryProviderProcessor<T>(provider.Session, null, null, provider.IndexName);
                ravenQueryProvider.ProcessExpression(expression);
	            return ((IRavenQueryInspector) ravenQueryProvider.LuceneQuery).IndexQueried;
	        }
	    }

        /// <summary>
        /// Grant access to the query session
        /// </summary>
	    public IDocumentSession Session
	    {
            get { return provider.Session; }
	    }

	    ///<summary>
	    ///</summary>
	    public KeyValuePair<string, string> GetLastEqualityTerm()
	    {
            var ravenQueryProvider = new RavenQueryProviderProcessor<T>(provider.Session, null, null, provider.IndexName);
            ravenQueryProvider.ProcessExpression(expression);
	        return ((IRavenQueryInspector) ravenQueryProvider.LuceneQuery).GetLastEqualityTerm();
	    }
    }
}
