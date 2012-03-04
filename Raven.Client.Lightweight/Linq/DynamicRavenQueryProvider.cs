//-----------------------------------------------------------------------
// <copyright file="DynamicRavenQueryProvider.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq.Expressions;
#if !NET_3_5
using Raven.Client.Connection.Async;
#endif
using Raven.Client.Connection;

namespace Raven.Client.Linq
{
	/// <summary>
	/// This is a specialized query provider for querying dynamic indexes
	/// </summary>
	public class DynamicRavenQueryProvider<T> : RavenQueryProvider<T>
	{
		/// <summary>
		/// Creates a dynamic query provider around the provided document session
		/// </summary>
		public DynamicRavenQueryProvider(
			IDocumentQueryGenerator queryGenerator,
			string indexName,
			RavenQueryStatistics ravenQueryStatistics
#if !SILVERLIGHT
, IDatabaseCommands databaseCommands
#endif
#if !NET_3_5
, IAsyncDatabaseCommands asyncDatabaseCommands
#endif
)
			: base(queryGenerator, indexName, ravenQueryStatistics
#if !SILVERLIGHT
, databaseCommands
#endif
#if !NET_3_5
, asyncDatabaseCommands
#endif
			)
		{
		}
		
		protected override RavenQueryProviderProcessor<S> GetQueryProviderProcessor<S>()
		{
			return new DynamicQueryProviderProcessor<S>(queryGenerator, customizeQuery, afterQueryExecuted, indexName, FieldsToFetch, FieldsToRename);
		}

		/// <summary>
		/// Convert the expression to a Lucene query
		/// </summary>
		public IDocumentQuery<TResult> ToLuceneQuery<TResult>(Expression expression)
		{
			var processor = GetQueryProviderProcessor<T>();
			return (IDocumentQuery<TResult>)processor.GetLuceneQueryFor(expression);
		}
	}
}