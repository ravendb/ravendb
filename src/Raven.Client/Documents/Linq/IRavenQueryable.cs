//-----------------------------------------------------------------------
// <copyright file="IRavenQueryable.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Documents.Queries.Highlighting;
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


        /// <inheritdoc cref="IRavenQueryable{T}.Highlight(string,int,int, HighlightingOptions, out Highlightings)"/>
        IRavenQueryable<T> Highlight(string fieldName, int fragmentLength, int fragmentCount, out Highlightings highlightings);

        /// <summary>
        /// Request to get the list of text fragments that highlight the searched terms when a making a Full-Text Search query.
        /// Note: The usage of a static index and highlighting together requires that the field on which you search must be configured for highlighting.
        /// </summary>
        /// <param name="fieldName">Name of the field that contains the searched terms to highlight</param>
        /// <param name="fragmentLength">Maximum length of a text fragment</param>
        /// <param name="fragmentCount">Maximum number of text fragments that will be returned</param>
        /// <param name="options">Customized highlighting options</param>
        /// <param name="highlightings">The 'out' parameter that will contain the highlighted text fragments for each returned result</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HighlightQueryResults"/>
        IRavenQueryable<T> Highlight(string fieldName, int fragmentLength, int fragmentCount, HighlightingOptions options, out Highlightings highlightings);

        /// <inheritdoc cref="IRavenQueryable{T}.Highlight(Expression{Func{T, object}},int,int, HighlightingOptions, out Highlightings)"/>
        IRavenQueryable<T> Highlight(Expression<Func<T, object>> path, int fragmentLength, int fragmentCount, out Highlightings highlightings);

        /// <summary>
        /// Request to get the list of text fragments that highlight the searched terms when a making a Full-Text Search query.
        /// Note: The usage of a static index and highlighting together requires that the field on which you search must be configured for highlighting.
        /// </summary>
        /// <param name="path">Path to the field that contains the searched terms to highlight</param>
        /// <param name="fragmentLength">Maximum length of a text fragment</param>
        /// <param name="fragmentCount">Maximum number of text fragments that will be returned</param>
        /// <param name="options">Customized highlighting options</param>
        /// <param name="highlightings">The 'out' parameter that will contain the highlighted text fragments for each returned result</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HighlightQueryResults"/>
        IRavenQueryable<T> Highlight(Expression<Func<T, object>> path, int fragmentLength, int fragmentCount, HighlightingOptions options, out Highlightings highlightings);
    }
}
