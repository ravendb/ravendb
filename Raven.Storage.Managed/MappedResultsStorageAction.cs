using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Storage.StorageActions;
using Raven.Storage.Managed.Impl;
using System.Linq;

namespace Raven.Storage.Managed
{
    public class MappedResultsStorageAction : IMappedResultsStorageAction
    {
        private readonly TableStorage storage;

        public MappedResultsStorageAction(TableStorage storage)
        {
            this.storage = storage;
        }

        public void PutMappedResult(string view, string docId, string reduceKey, JObject data, byte[] viewAndReduceKeyHashed)
        {
            var ms = new MemoryStream();
            data.WriteTo(new BsonWriter(ms));
            storage.MappedResults.Put(new JObject
            {
                {"view", view},
                {"reduceKey", reduceKey},
                {"docId", docId},
                {"mapResultId", DocumentDatabase.CreateSequentialUuid().ToByteArray()}
            }, ms.ToArray());
        }

        public IEnumerable<JObject> GetMappedResults(string view, string reduceKey, byte[] viewAndReduceKeyHashed)
        {
            return storage.MappedResults["ByViewAndReduceKey"].SkipAfter(new JObject
            {
                {"view", view},
                {"reduceKey", reduceKey}
            }).TakeWhile(x => StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("view"), view) &&
                              StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("reduceKey"), reduceKey))
                .Select(x =>
                {
                    var readResult = storage.MappedResults.Read(x);
                    if (readResult == null)
                        return null;
                    return (JObject) JToken.ReadFrom(new BsonReader(new MemoryStream(readResult.Data())));
                }).Where(x => x != null);

        }

        public IEnumerable<string> DeleteMappedResultsForDocumentId(string documentId, string view)
        {
            foreach (var key in storage.MappedResults["ByViewAndDocumentId"].SkipAfter(new JObject
            {
                {"view", view},
                {"docId", documentId},
            }).TakeWhile(x => StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("view"), view) &&
                              StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("docId"), documentId)))
            {
                storage.MappedResults.Remove(key);
                yield return key.Value<string>("reduceKey");
            };
        }

        public void DeleteMappedResultsForView(string view)
        {
            foreach (var key in storage.MappedResults["ByViewAndReduceKey"].SkipAfter(new JObject
            {
                {"view", view},
            }).TakeWhile(x => StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("view"), view)))
            {
                storage.MappedResults.Remove(key);
            }
        }
    }
}