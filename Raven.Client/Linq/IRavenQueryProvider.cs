using System;
using System.Linq;
using Raven.Database.Data;

namespace Raven.Client.Linq
{
	/// <summary>
	/// Extension for the builtin <see cref="IQueryProvider"/> allowing for Raven specific operations
	/// </summary>
    public interface IRavenQueryProvider : IQueryProvider
	{
        /// <summary>
        /// Callback to get the results of the query
        /// </summary>
	    void AfterQueryExecuted(Action<QueryResult> afterQueryExecuted);

		/// <summary>
		/// Customizes the query using the specified action
		/// </summary>
        void Customize(Action<IDocumentQueryCustomization> action);

		/// <summary>
		/// Gets the session.
		/// </summary>
		/// <value>The session.</value>
        IDocumentSession Session { get; }
		/// <summary>
		/// Gets the name of the index.
		/// </summary>
		/// <value>The name of the index.</value>
        string IndexName { get; }

        /// <summary>
        /// Change the result type for the query provider
        /// </summary>
	    IRavenQueryProvider For<S>();
    }
}
