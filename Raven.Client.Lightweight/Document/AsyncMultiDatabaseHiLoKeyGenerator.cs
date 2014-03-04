// -----------------------------------------------------------------------
//  <copyright file="AsyncMultiDatabaseHiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;

namespace Raven.Client.Document
{
	public class AsyncMultiDatabaseHiLoKeyGenerator
	{
		private readonly int capacity;

		private readonly ConcurrentDictionary<string, AsyncMultiTypeHiLoKeyGenerator> generators =
			new ConcurrentDictionary<string, AsyncMultiTypeHiLoKeyGenerator>();

		public AsyncMultiDatabaseHiLoKeyGenerator(int capacity)
		{
			this.capacity = capacity;
		}

		public Task<string> GenerateDocumentKeyAsync(string dbName, IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions,
													 object entity)
		{
			var generator = generators.GetOrAdd(dbName ?? Constants.SystemDatabase, s => new AsyncMultiTypeHiLoKeyGenerator(capacity));
			return generator.GenerateDocumentKeyAsync(databaseCommands, conventions, entity);
		}
	}
}