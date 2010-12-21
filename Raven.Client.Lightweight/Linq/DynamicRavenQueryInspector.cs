//-----------------------------------------------------------------------
// <copyright file="DynamicRavenQueryInspector.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
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
    public class DynamicRavenQueryInspector<T> : RavenQueryInspector<T>    
    {
        /// <summary>
        /// Creates a dynamic raven queryable around the provided query provider
        /// </summary>
        public DynamicRavenQueryInspector(IRavenQueryProvider queryProvider, RavenQueryStatistics queryStatistics)
            : base(queryProvider, queryStatistics)
        {

        }

        /// <summary>
        /// Creates a dynamic raven queryable around the provided query provider + expression
        /// </summary>
        public DynamicRavenQueryInspector(IRavenQueryProvider queryProvider, Expression expression, RavenQueryStatistics queryStatistics)
            : base(queryProvider, expression, queryStatistics)  
        {

        }


    }
}
