using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;

namespace Raven.Client.Linq
{
    /// <summary>
    /// A specialised queryable object for querying dynamic indexes
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DynamicRavenQueryable<T> : RavenQueryable<T>    
    {
        /// <summary>
        /// Creates a dynamic raven queryable around the provided query provider
        /// </summary>
        /// <param name="queryProvider"></param>
        public DynamicRavenQueryable(IRavenQueryProvider queryProvider) 
            : base(queryProvider)
        {

        }

        /// <summary>
        /// Creates a dynamic raven queryable around the provided query provider + expression
        /// </summary>
        /// <param name="queryProvider"></param>
        /// <param name="expression"></param>
        public DynamicRavenQueryable(IRavenQueryProvider queryProvider, Expression expression)
            : base(queryProvider, expression)
        {

        }


    }
}
