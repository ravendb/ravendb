using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Client.Linq
{
    public static class LindExtensions
    {
        public static IEnumerable<TResult> As<TResult>(this IQueryable queryable)
        {
            var results = queryable.Provider.CreateQuery<TResult>(queryable.Expression);
            ((RavenQueryable<TResult>)results).Customize(x => x.SelectFields<TResult>(null));
            return results;
        }
    }
}