using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Storage.StorageActions;
using Raven.Storage.Managed.Impl;
using System.Linq;

namespace Raven.Storage.Managed
{
    public class QueueStorageActions : IQueueStorageActions
    {
        private readonly TableStorage storage;

        public QueueStorageActions(TableStorage storage)
        {
            this.storage = storage;
        }

        public void EnqueueToQueue(string name, byte[] data)
        {
            storage.Queues.Put(new JObject
            {
                {"name", name},
                {"id", DocumentDatabase.CreateSequentialUuid().ToByteArray()},
                {"reads", 0}
            }, data);
        }

        public IEnumerable<Tuple<byte[], object>> PeekFromQueue(string name)
        {
            foreach (var queuedMsgKey in storage.Queues["ByName"].SkipTo(new JObject
            {
                {"name", name}
            }).TakeWhile(x=> StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("name"), name)))
            {
                var readResult = storage.Queues.Read(queuedMsgKey);
                if(readResult == null)
                    yield break;

                readResult.Key["reads"] = readResult.Key.Value<int>("reads") + 1;

                if (readResult.Key.Value<int>("reads") > 5) //      // read too much, probably poison message, remove it
                {
                    storage.Queues.Remove(readResult.Key);
                    continue;
                }

                storage.Queues.UpdateKey(readResult.Key);

                yield return new Tuple<byte[], object>(
                    readResult.Data(),
                    readResult.Key.Value<byte[]>("id")
                    );
            }
        }

        public void DeleteFromQueue(string name, object id)
        {
            storage.Queues.Remove(new JObject
                {
                    {"name", name},
                    {"id", (byte[])id}
                });
        }
    }
}