//-----------------------------------------------------------------------
// <copyright file="IDocumentQueryCustomization.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq.Expressions;

namespace Raven.Client
{
	/// <summary>
	/// Customize the document query
	/// </summary>
	public interface IDocumentQueryCustomization
	{
		/// <summary>
		/// Instructs the query to wait for non stale results as of now.
		/// </summary>
		/// <returns></returns>
		IDocumentQueryCustomization WaitForNonStaleResultsAsOfNow();
		/// <summary>
		/// Instructs the query to wait for non stale results as of now for the specified timeout.
		/// </summary>
		/// <param name="waitTimeout">The wait timeout.</param>
		/// <returns></returns>
		IDocumentQueryCustomization WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout);

		/// <summary>
		/// Instructs the query to wait for non stale results as of the cutoff date.
		/// </summary>
		/// <param name="cutOff">The cut off.</param>
		/// <returns></returns>
		IDocumentQueryCustomization WaitForNonStaleResultsAsOf(DateTime cutOff);
		/// <summary>
		/// Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
		/// </summary>
		/// <param name="cutOff">The cut off.</param>
		/// <param name="waitTimeout">The wait timeout.</param>
		IDocumentQueryCustomization WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout);

		/// <summary>
		/// EXPERT ONLY: Instructs the query to wait for non stale results.
		/// This shouldn't be used outside of unit tests unless you are well aware of the implications
		/// </summary>
		IDocumentQueryCustomization WaitForNonStaleResults();


		/// <summary>
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name="path">The path.</param>
		IDocumentQueryCustomization Include<T>(Expression<Func<T, object>> path);


		/// <summary>
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name="path">The path.</param>
		IDocumentQueryCustomization Include(string path);

		/// <summary>
		/// EXPERT ONLY: Instructs the query to wait for non stale results for the specified wait timeout.
		/// This shouldn't be used outside of unit tests unless you are well aware of the implications
		/// </summary>
		/// <param name="waitTimeout">The wait timeout.</param>
		IDocumentQueryCustomization WaitForNonStaleResults(TimeSpan waitTimeout);
		/// <summary>
		/// Selects the specified fields directly from the index
		/// </summary>
		/// <typeparam name="TProjection">The type of the projection.</typeparam>
		/// <param name="fields">The fields.</param>
		IDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields);

		/// <summary>
		/// Filter matches to be inside the specified radius
		/// </summary>
		/// <param name="radius">The radius.</param>
		/// <param name="latitude">The latitude.</param>
		/// <param name="longitude">The longitude.</param>
		IDocumentQueryCustomization WithinRadiusOf(double radius, double latitude, double longitude);
	}
}