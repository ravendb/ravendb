//-----------------------------------------------------------------------
// <copyright file="IRavenQueryable.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Queries.Spatial;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Linq
{
    /// <summary>
    /// An implementation of <see cref="IOrderedQueryable{T}"/> with Raven specific operation
    /// </summary>
    public interface IRavenQueryable<T> : IOrderedQueryable<T>
    {
        /// <summary>
        /// Provide statistics about the query, such as duration, total number of results, staleness information, etc.
        /// </summary>
        IRavenQueryable<T> Statistics(out QueryStatistics stats);

        /// <summary>
        /// Customizes the query using the specified action
        /// </summary>
        IRavenQueryable<T> Customize(Action<IDocumentQueryCustomization> action);

        /// <summary>
        /// Inputs a key and value to the query (accessible by the transformer)
        /// </summary>
        IRavenQueryable<T> AddTransformerParameter(string name, object value);

        IRavenQueryable<T> Spatial(Expression<Func<T, object>> path, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

        IRavenQueryable<T> OrderByDistance(SpatialSort sortParamsClause);
    }
}
