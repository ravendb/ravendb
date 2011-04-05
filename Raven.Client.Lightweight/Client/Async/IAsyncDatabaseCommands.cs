//-----------------------------------------------------------------------
// <copyright file="IAsyncDatabaseCommands.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET_3_5

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Json.Linq;

namespace Raven.Client.Client.Async
{
	/// <summary>
	/// An async database command operations
	/// </summary>
	public interface IAsyncDatabaseCommands : IDisposable
	{
		/// <summary>
		/// Gets or sets the operations headers.
		/// </summary>
		/// <value>The operations headers.</value>
		IDictionary<string,string> OperationsHeaders { get;  }

		/// <summary>
		/// Begins an async get operation
		/// </summary>
		/// <param name="key">The key.</param>
		Task<JsonDocument> GetAsync(string key);
		
		/// <summary>
		/// Begins an async multi get operation
		/// </summary>
		/// <param name="keys">The keys.</param>
		Task<JsonDocument[]> MultiGetAsync(string[] keys);

		/// <summary>
		/// Begins an async get operation for documents
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		/// <remarks>
		/// This is primarily useful for administration of a database
		/// </remarks>
		Task<JsonDocument[]> GetDocumentsAsync(int start, int pageSize);

		/// <summary>
		/// Begins an async get operation for documents whose id starts with the specified prefix
		/// </summary>
		/// <param name="prefix">Prefix that the ids begin with.</param>
		/// <param name="start">Paging start.</param>
		/// <param name="pageSize">Size of the page.</param>
		/// <remarks>
		/// This is primarily useful for administration of a database
		/// </remarks>
		Task<JsonDocument[]> GetDocumentsStartingWithAsync(string prefix, int start, int pageSize);

		/// <summary>
		/// Begins the async query.
		/// </summary>
		/// <param name="index">The index.</param>
		/// <param name="query">The query.</param>
		/// <param name="includes">The include paths</param>
		Task<QueryResult> QueryAsync(string index, IndexQuery query, string[] includes);

		/// <summary>
		/// Begins the async query.
		/// </summary>
		/// <param name="query">A string representation of a Linq query</param>
		Task<QueryResult> LinearQueryAsync(string query, int start, int pageSize);

		/// <summary>
		/// Begins the async batch operation
		/// </summary>
		/// <param name="commandDatas">The command data.</param>
		Task<BatchResult[]> BatchAsync(ICommandData[] commandDatas);

		/// <summary>
		/// Returns a list of suggestions based on the specified suggestion query.
		/// </summary>
		/// <param name="index">The index to query for suggestions</param>
		/// <param name="suggestionQuery">The suggestion query.</param>
		Task<SuggestionQueryResult> SuggestAsync(string index, SuggestionQuery suggestionQuery);

		/// <summary>
		/// Gets the index names from the server asyncronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		Task<string[]> GetIndexNamesAsync(int start, int pageSize);

		/// <summary>
		/// Gets the indexes from the server asyncronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		Task<IndexDefinition[]> GetIndexesAsync(int start, int pageSize);

		/// <summary>
		/// Resets the specified index asyncronously
		/// </summary>
		/// <param name="name">The name.</param>
		Task ResetIndexAsync(string name);

		/// <summary>
		/// Gets the index definition for the specified name asyncronously
		/// </summary>
		/// <param name="name">The name.</param>
		Task<IndexDefinition> GetIndexAsync(string name);

		/// <summary>
		/// Puts the index definition for the specified name asyncronously
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <param name="overwrite">Should overwrite index</param>
		Task<string> PutIndexAsync(string name, IndexDefinition indexDef, bool overwrite);

		/// <summary>
		/// Deletes the index definition for the specified name asyncronously
		/// </summary>
		/// <param name="name">The name.</param>
		Task DeleteIndexAsync(string name);

		/// <summary>
		/// Deletes the document for the specified id asyncronously
		/// </summary>
		/// <param name="id">The id.</param>
		Task DeleteDocumentAsync(string id);

		/// <summary>
		/// Puts the document with the specified key in the database
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="document">The document.</param>
		/// <param name="metadata">The metadata.</param>
		Task<PutResult> PutAsync(string key, Guid? etag, RavenJObject document, RavenJObject metadata);

		/// <summary>
		/// Create a new instance of <see cref="IAsyncDatabaseCommands"/> that will interacts
		/// with the specified database
		/// </summary>
		IAsyncDatabaseCommands ForDatabase(string database);

		/// <summary>
		/// Returns a new <see cref="IDatabaseCommands "/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		IAsyncDatabaseCommands With(ICredentials credentialsForSession);

		/// <summary>
		/// Retrieve the statistics for the database asynchronously
		/// </summary>
		Task<DatabaseStatistics> GetStatisticsAsync();

		/// <summary>
		/// Gets the list of databases from the server asyncronously
		/// </summary>
		Task<string[]> GetDatabaseNamesAsync();

		/// <summary>
		/// Gets the list of collections from the server asyncronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		Task<Collection[]> GetCollectionsAsync(int start, int pageSize);

		/// <summary>
		/// Puts the attachment with the specified key asyncronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="data">The data.</param>
		/// <param name="metadata">The metadata.</param>
		Task PutAttachmentAsync(string key, Guid? etag, byte[] data, RavenJObject metadata);

		/// <summary>
		/// Gets the attachment by the specified key asyncronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		Task<Attachment> GetAttachmentAsync(string key);

		/// <summary>
		/// Deletes the attachment with the specified key asyncronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		Task DeleteAttachmentAsync(string key, Guid? etag);

        ///<summary>
        /// Get the possible terms for the specified field in the index asynchronously
        /// You can page through the results by use fromValue parameter as the 
        /// starting point for the next query
        ///</summary>
        ///<returns></returns>
        Task<string[]> GetTermsAsync(string index, string field, string fromValue, int pageSize);

		/// <summary>
		/// Ensures that the silverlight startup tasks have run
		/// </summary>
		Task EnsureSilverlightStartUpAsync();
	}
}
#endif