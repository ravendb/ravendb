// -----------------------------------------------------------------------
//  <copyright file="MultiDatabaseHiLoGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Concurrent;
using Raven.Abstractions.Data;
using Raven.Client.Connection;

namespace Raven.Client.Document
{
	public class MultiDatabaseHiLoGenerator
	{
		private readonly int capacity;

		private readonly ConcurrentDictionary<string, MultiTypeHiLoKeyGenerator> generators =
			new ConcurrentDictionary<string, MultiTypeHiLoKeyGenerator>();

		public MultiDatabaseHiLoGenerator(int capacity)
		{
			this.capacity = capacity;
		}

		public string GenerateDocumentKey(string dbName, IDatabaseCommands databaseCommands, DocumentConvention conventions, object entity)
		{
			var multiTypeHiLoKeyGenerator = generators.GetOrAdd(dbName ?? Constants.SystemDatabase, s => new MultiTypeHiLoKeyGenerator(capacity));
			return multiTypeHiLoKeyGenerator.GenerateDocumentKey(databaseCommands, conventions, entity);
		}
	}
}