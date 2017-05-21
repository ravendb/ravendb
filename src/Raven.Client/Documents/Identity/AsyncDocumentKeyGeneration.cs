// -----------------------------------------------------------------------
//  <copyright file="AsyncDocumentKeyGeneration.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Identity
{
    internal class AsyncDocumentKeyGeneration
    {
        private readonly LinkedList<object> _entitiesStoredWithoutIDs = new LinkedList<object>();

        public delegate bool TryGetValue(object key, out DocumentInfo documentInfo);

        public delegate string ModifyObjectId(string id, object entity, BlittableJsonReaderObject metadata);

        private readonly InMemoryDocumentSessionOperations _session;
        private readonly TryGetValue _tryGetValue;
        private readonly ModifyObjectId _modifyObjectId;

        public AsyncDocumentKeyGeneration(InMemoryDocumentSessionOperations session, TryGetValue tryGetValue, ModifyObjectId modifyObjectId)
        {
            _session = session;
            _tryGetValue = tryGetValue;
            _modifyObjectId = modifyObjectId;
        }

        public Task GenerateDocumentKeysForSaveChanges()
        {
            if (_entitiesStoredWithoutIDs.Count != 0)
            {
                var entity = _entitiesStoredWithoutIDs.First.Value;
                _entitiesStoredWithoutIDs.RemoveFirst();

                DocumentInfo documentInfo;
                if (_tryGetValue(entity, out documentInfo))
                {
                    return _session.GenerateDocumentKeyForStorageAsync(entity)
                        .ContinueWith(task => documentInfo.Id = _modifyObjectId(task.Result, entity, documentInfo.Metadata))
                        .ContinueWithTask(GenerateDocumentKeysForSaveChanges);
                }
            }

            return Task.CompletedTask;
        }

        public void Add(object entity)
        {
            _entitiesStoredWithoutIDs.AddLast(entity);
        }
    }
}
