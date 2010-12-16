using System;
using System.ComponentModel;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Database.Exceptions;
using Raven.Database.Storage;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
    public class StalenessStorageActions : IStalenessStorageActions
    {
        private readonly TableStorage storage;

        public StalenessStorageActions(TableStorage storage)
        {
            this.storage = storage;
        }

        public bool IsIndexStale(string name, DateTime? cutOff, string entityName)
        {
            var readResult = storage.IndexingStats.Read(new JObject
            {
                {"index", name}
            });

            if (readResult == null)
                return false;// index does not exists

            var lastIndexedEtag = readResult.Key.Value<byte[]>("lastEtag");
            var lastIndexedTime = readResult.Key.Value<DateTime>("lastTimestamp");

            if (IsStaleByEtag(entityName, lastIndexedEtag))
            {
                if (cutOff == null)
                    return true;
                if (cutOff.Value >= lastIndexedTime)
                    return true;
            }

            var keyToSearch = new JObject
            {
                {"index", name},
            };
            var tasksAfterCutoffPoint = storage.Tasks["ByIndexAndTime"].SkipTo(keyToSearch);
            if (cutOff != null)
                tasksAfterCutoffPoint = tasksAfterCutoffPoint
                    .Where(x => x.Value<DateTime>("time") < cutOff.Value);
            return tasksAfterCutoffPoint.Any();
        }

        public Tuple<DateTime,Guid> IndexLastUpdatedAt(string name)
        {
            var readResult = storage.IndexingStats.Read(new JObject
            {
                {"index", name}
            });

            if (readResult == null)
                throw new IndexDoesNotExistsException("Could not find index named: " + name);

            return Tuple.Create(
                readResult.Key.Value<DateTime>("lastTimestamp"),
                new Guid(readResult.Key.Value<byte[]>("lastEtag"))
                );
        }

        private bool IsStaleByEtag(string entityName, byte [] lastIndexedEtag)
        {
            foreach (var doc in storage.Documents["ByEtag"].SkipFromEnd(0))
            {
                var docEtag = doc.Value<byte[]>("etag");
                if (CompareArrays(docEtag, lastIndexedEtag) <= 0)
                    break;
                if(entityName != null && 
                   StringComparer.InvariantCultureIgnoreCase.Equals(entityName, doc.Value<string>("entityName")) == false)
                    continue;
                return true;
            }
            return false;
        }


        private static int CompareArrays(byte[] docEtagBinary, byte[] indexEtagBinary)
        {
            for (int i = 0; i < docEtagBinary.Length; i++)
            {
                if (docEtagBinary[i].CompareTo(indexEtagBinary[i]) != 0)
                {
                    return docEtagBinary[i].CompareTo(indexEtagBinary[i]);
                }
            }
            return 0;
        }
    }
}