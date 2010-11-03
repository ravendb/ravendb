using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;

namespace Raven.Client.Linq
{
    /// <summary>
    /// A specialized queryable object for querying dynamic indexes
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DynamicRavenQueryable<T> : RavenQueryable<T>    
    {
        /// <summary>
        /// Creates a dynamic raven queryable around the provided query provider
        /// </summary>
        public DynamicRavenQueryable(IRavenQueryProvider queryProvider, RavenQueryStatistics queryStatistics)
            : base(queryProvider, queryStatistics)
        {

        }

        /// <summary>
        /// Creates a dynamic raven queryable around the provided query provider + expression
        /// </summary>
        public DynamicRavenQueryable(IRavenQueryProvider queryProvider, Expression expression, RavenQueryStatistics queryStatistics)
            : base(queryProvider, expression, queryStatistics)  
        {

        }


    }
}
