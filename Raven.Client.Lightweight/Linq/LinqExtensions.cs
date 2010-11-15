using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Client;

namespace Raven.Client.Linq
{
    ///<summary>
    /// Extensions to the linq syntax
    ///</summary>
    public static class LinqExtensions
    {
        /// <summary>
        /// Project using a different type
        /// </summary>
        public static IEnumerable<TResult> As<TResult>(this IQueryable queryable)
        {
            var results = queryable.Provider.CreateQuery<TResult>(queryable.Expression);
            ((RavenQueryInspector<TResult>)results).Customize(x => x.SelectFields<TResult>(null));
            return results;
        }

        /// <summary>
        /// Suggest alternative values for the queried term
        /// </summary>
        public static SuggestionQueryResult Suggest(this IQueryable queryable)
        {
            return Suggest(queryable, new SuggestionQuery());
        }

        /// <summary>
        /// Suggest alternative values for the queried term
        /// </summary>
        public static SuggestionQueryResult Suggest(this IQueryable queryable, SuggestionQuery query)
        {
            var queryInspector = (IRavenQueryInspector) queryable;
            var lastEqualityTerm = queryInspector.GetLastEqualityTerm();
            if(lastEqualityTerm.Key == null)
                throw new InvalidOperationException("Could not suggest on a query that doesn't have a single equality check");

            query.Field = lastEqualityTerm.Key;
            query.Term = lastEqualityTerm.Value;

            return queryInspector.Session.Advanced.DatabaseCommands.Suggest(queryInspector.IndexQueried, query);
        }

        /// <summary>
        /// Marker method for allowing complex (multi entity) queries on the server.
        /// </summary>
        public static IEnumerable<TResult> WhereEntityIs<TResult>(this IEnumerable<object> queryable, params string[] names)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }
    }
}
