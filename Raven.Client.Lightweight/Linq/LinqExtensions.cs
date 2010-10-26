using System.Collections;
using System.Collections.Generic;
using System.Linq;

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
            ((RavenQueryable<TResult>)results).Customize(x => x.SelectFields<TResult>(null));
            return results;
        }
    }
}
