//-----------------------------------------------------------------------
// <copyright file="MultiTypeHiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET35 && !SILVERLIGHT
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;

namespace Raven.Client.Document
{
	/// <summary>
	/// Generate a hilo key for each given type
	/// </summary>
	public class AsyncMultiTypeHiLoKeyGenerator
	{
		private readonly IDocumentStore documentStore;
		private readonly int capacity;
		private readonly object generatorLock = new object();
		private readonly ConcurrentDictionary<string, AsyncHiLoKeyGenerator> keyGeneratorsByTag = new ConcurrentDictionary<string, AsyncHiLoKeyGenerator>();


		public AsyncMultiTypeHiLoKeyGenerator(int capacity)
		{
			this.capacity = capacity;
		}

		public AsyncMultiTypeHiLoKeyGenerator(IDocumentStore documentStore, int capacity)
		{
			this.documentStore = documentStore;
			this.capacity = capacity;
		}
		
		public Task<string> GenerateDocumentKeyAsync(IAsyncDatabaseCommands databaseCommands, DocumentConvention conventions, object entity)
		{
		    var typeTagName = conventions.GetTypeTagName(entity.GetType());
			if (string.IsNullOrEmpty(typeTagName)) //ignore empty tags
				return CompletedTask.With<string>(null);
			var tag = conventions.TransformTypeTagNameToDocumentKeyPrefix(typeTagName);
			AsyncHiLoKeyGenerator value;
			if (keyGeneratorsByTag.TryGetValue(tag, out value))
				return value.GenerateDocumentKeyAsync(databaseCommands, conventions, entity);

			lock(generatorLock)
			{
				if (keyGeneratorsByTag.TryGetValue(tag, out value))
					return value.GenerateDocumentKeyAsync(databaseCommands, conventions, entity);

				value = new AsyncHiLoKeyGenerator(tag, capacity);
				keyGeneratorsByTag.TryAdd(tag, value);
			}

			return value.GenerateDocumentKeyAsync(databaseCommands, conventions, entity);
		}
	}
}
#endif
