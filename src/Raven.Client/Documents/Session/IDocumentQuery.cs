//-----------------------------------------------------------------------
// <copyright file="IDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Queries.Suggestions;

namespace Raven.Client.Documents.Session
{
    public interface IDocumentQueryBase<T>
    {
        /// <summary>
        ///     Register the query as a lazy-count query in the session and return a lazy
        ///     instance that will evaluate the query only when needed.
        /// </summary>
        Lazy<int> CountLazily();

        /// <summary>
        ///     Returns first element or throws if sequence is empty.
        /// </summary>
        T First();

        /// <summary>
        ///     Returns first element or default value for type if sequence is empty.
        /// </summary>
        T FirstOrDefault();

        /// <summary>
        ///     Returns first element or throws if sequence is empty or contains more than one element.
        /// </summary>
        T Single();

        /// <summary>
        ///     Returns first element or default value for given type if sequence is empty. Throws if sequence contains more than
        ///     one element.
        /// </summary>
        T SingleOrDefault();

        /// <summary>
        ///     Checks if the given query matches any records
        /// </summary>
        bool Any();

        /// <summary>
        /// Gets the total count of records for this query
        /// </summary>
        int Count();

        /// <summary>
        ///     Register the query as a lazy query in the session and return a lazy
        ///     instance that will evaluate the query only when needed.
        ///     Also provide a function to execute when the value is evaluated
        /// </summary>
        Lazy<IEnumerable<T>> Lazily(Action<IEnumerable<T>> onEval = null);
    }

    public interface IRawDocumentQuery<T> :
        IQueryBase<T, IRawDocumentQuery<T>>,
        IDocumentQueryBase<T>, IEnumerable<T>
    {
    }

    public interface IGraphQuery<T> :
        IQueryBase<T, IGraphQuery<T>>,
        IDocumentQueryBase<T>, IEnumerable<T>
    {
        IGraphQuery<T> With<TOther>(string alias, IRavenQueryable<TOther> query);
    }

    /// <summary>
    ///     A query against a Raven index
    /// </summary>
    public interface IDocumentQuery<T> : 
        IEnumerable<T>, 
        IDocumentQueryBase<T, IDocumentQuery<T>>,
        IDocumentQueryBase<T>
    {
        string IndexName { get; }

        /// <summary>
        ///     Whether we should apply distinct operation to the query on the server side
        /// </summary>
        bool IsDistinct { get; }

        IRavenQueryable<T> ToQueryable();

        /// <summary>
        ///     Returns the query result. Accessing this property for the first time will execute the query.
        /// </summary>
        QueryResult GetQueryResult();

        /// <summary>
        ///     Selects the specified fields directly from the index if the are stored. If the field is not stored in index, value
        ///     will come from document directly.
        /// </summary>
        /// <typeparam name="TProjection">Type of the projection.</typeparam>
        /// <param name="fields">Array of fields to load.</param>
        IDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields);

        /// <summary>
        ///     Selects the specified fields directly from the index if the are stored. If the field is not stored in index, value
        ///     will come from document directly.
        /// </summary>
        /// <typeparam name="TProjection">Type of the projection.</typeparam>
        /// <param name="queryData">An object containing the fields to load, field projections and a From-Token alias name</param>
        IDocumentQuery<TProjection> SelectFields<TProjection>(QueryData queryData);

        /// <summary>
        ///     Selects the specified fields directly from the index if the are stored. If the field is not stored in index, value
        ///     will come from document directly.
        ///     <para>Array of fields will be taken from TProjection</para>
        /// </summary>
        /// <typeparam name="TProjection">Type of the projection from which fields will be taken.</typeparam>
        IDocumentQuery<TProjection> SelectFields<TProjection>();

        /// <summary>
        /// Changes the return type of the query
        /// </summary>
        IDocumentQuery<TResult> OfType<TResult>();

        IGroupByDocumentQuery<T> GroupBy(string fieldName, params string[] fieldNames);

        IGroupByDocumentQuery<T> GroupBy((string Name, GroupByMethod Method) field, params (string Name, GroupByMethod Method)[] fields);

        IDocumentQuery<T> MoreLikeThis(Action<IMoreLikeThisBuilderForDocumentQuery<T>> builder);

        IAggregationDocumentQuery<T> AggregateBy(Action<IFacetBuilder<T>> builder);

        IAggregationDocumentQuery<T> AggregateBy(FacetBase facet);

        IAggregationDocumentQuery<T> AggregateBy(IEnumerable<Facet> facets);

        IAggregationDocumentQuery<T> AggregateUsing(string facetSetupDocumentId);

        ISuggestionDocumentQuery<T> SuggestUsing(SuggestionBase suggestion);

        ISuggestionDocumentQuery<T> SuggestUsing(Action<ISuggestionBuilder<T>> builder);
    }
}
