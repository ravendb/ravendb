// -----------------------------------------------------------------------
//  <copyright file="AsyncDocumentKeyGeneration.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Extensions;
using Raven.NewClient.Json.Linq;
using Sparrow.Json;
using DocumentInfo = Raven.NewClient.Client.Documents.InMemoryDocumentSessionOperations.DocumentInfo;

namespace Raven.NewClient.Client.Documents.Async
{
    public class AsyncDocumentKeyGeneration
    {
        private readonly LinkedList<object> entitiesStoredWithoutIDs = new LinkedList<object>();

        public delegate bool TryGetValue(object key, out DocumentInfo documentInfo);

        public delegate string ModifyObjectId(string id, object entity, BlittableJsonReaderObject metadata);

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

                DocumentInfo documentInfo;
                if (tryGetValue(entity, out documentInfo))
                {
                    return session.GenerateDocumentKeyForStorageAsync(entity)
                        .ContinueWith(task => documentInfo.Id = modifyObjectId(task.Result, entity, documentInfo.Metadata))
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
