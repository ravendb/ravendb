// -----------------------------------------------------------------------
//  <copyright file="AsyncDocumentKeyGeneration.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
#if !NET35
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Document.Async
{
	public class AsyncDocumentKeyGeneration
	{
		private readonly LinkedList<object> entitiesStoredWithoutIDs = new LinkedList<object>();

		public delegate bool TryGetValue(object key, out InMemoryDocumentSessionOperations.DocumentMetadata metadata);

		public delegate string ModifyObjectId(string id, object entity, RavenJObject metadata);

		private readonly InMemoryDocumentSessionOperations session;
		private readonly TryGetValue tryGetValue;
		private readonly ModifyObjectId modifyObjectId;

		public AsyncDocumentKeyGeneration(InMemoryDocumentSessionOperations session,TryGetValue tryGetValue, ModifyObjectId modifyObjectId)
		{
			this.session = session;
			this.tryGetValue = tryGetValue;
			this.modifyObjectId = modifyObjectId;
		}

		public Task GenerateDocumentKeysForSaveChanges()
		{
			if (entitiesStoredWithoutIDs.Count != 0)
			{
				var entity = entitiesStoredWithoutIDs.First.Value;
				entitiesStoredWithoutIDs.RemoveFirst();

				InMemoryDocumentSessionOperations.DocumentMetadata metadata;
				if (tryGetValue(entity, out metadata))
				{
					return session.GenerateDocumentKeyForStorageAsync(entity)
						.ContinueWith(task => metadata.Key = modifyObjectId(task.Result, entity, metadata.Metadata))
						.ContinueWithTask(GenerateDocumentKeysForSaveChanges);
				}
			}

			return new CompletedTask();
		}

		public void Add(object entity)
		{
			entitiesStoredWithoutIDs.AddLast(entity);
		}
	}
}
#endif