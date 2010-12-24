//-----------------------------------------------------------------------
// <copyright file="IAsyncDatabaseCommands.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET_3_5

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Database;
using Raven.Database.Data;

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
		/// Begins the async query.
		/// </summary>
		/// <param name="index">The index.</param>
		/// <param name="query">The query.</param>
		/// <param name="includes">The include paths</param>
		Task<QueryResult> QueryAsync(string index, IndexQuery query, string[] includes);

		/// <summary>
		/// Begins the async batch operation
		/// </summary>
		/// <param name="commandDatas">The command data.</param>
		Task<BatchResult[]> BatchAsync(ICommandData[] commandDatas);

		/// <summary>
		/// Create a new instance of <see cref="IAsyncDatabaseCommands"/> that will interacts
		/// with the specified database
		/// </summary>
		IAsyncDatabaseCommands ForDatabase(string database);
	}
}
#endif