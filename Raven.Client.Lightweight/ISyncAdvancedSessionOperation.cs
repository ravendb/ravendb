#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Client.Client;
#if !NET_3_5
using Raven.Client.Client.Async;
#endif
using Raven.Client.Indexes;

namespace Raven.Client
{
	/// <summary>
	/// Advanced synchronous session operations
	/// </summary>
	public interface ISyncAdvancedSessionOperation : IAdvancedDocumentSessionOperations
	{
		/// <summary>
		/// Refreshes the specified entity from Raven server.
		/// </summary>
		/// <param name="entity">The entity.</param>
		void Refresh<T>(T entity);

		/// <summary>
		/// Gets the database commands.
		/// </summary>
		/// <value>The database commands.</value>
		IDatabaseCommands DatabaseCommands { get; }

#if !NET_3_5
		/// <summary>
		/// Gets the async database commands.
		/// </summary>
		/// <value>The async database commands.</value>
		IAsyncDatabaseCommands AsyncDatabaseCommands { get; }
#endif

		/// <summary>
		/// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		/// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
		/// <returns></returns>
		IDocumentQuery<T> LuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new();

		/// <summary>
		/// Query the specified index using Lucene syntax
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		IDocumentQuery<T> LuceneQuery<T>(string indexName);

		/// <summary>
		/// Dynamically query RavenDB using Lucene syntax
		/// </summary>
		IDocumentQuery<T> LuceneQuery<T>();

		/// <summary>
		/// Gets the document URL for the specified entity.
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
		string GetDocumentUrl(object entity);
	}
}
#endif
