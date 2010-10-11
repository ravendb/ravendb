using System;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Exceptions;
using Raven.Database.Json;
using Raven.Database.Plugins;

namespace Raven.Bundles.Replication.Triggers
{
    public class AncestryPutTrigger : AbstractPutTrigger
    {
        private long currentLo = Capacity + 1;
        private readonly object generatorLock = new object();
        private long currentHi;
        private const long Capacity = 1024*16;

        public override void OnPut(string key, JObject document, JObject metadata, TransactionInformation transactionInformation)
        {
            if (key.StartsWith("Raven/")) // we don't deal with system documents
                return;
            if (ReplicationContext.IsInReplicationContext)
                return;
            var doc = Database.Get(key, null);
            if (doc != null)
            {
                metadata[ReplicationConstants.RavenReplicationParentVersion] =
                    doc.Metadata[ReplicationConstants.RavenReplicationVersion];
                metadata[ReplicationConstants.RavenReplicationParentSource] =
                doc.Metadata[ReplicationConstants.RavenReplicationSource];
            }
            metadata[ReplicationConstants.RavenReplicationVersion] = JToken.FromObject(NextId());
            metadata[ReplicationConstants.RavenReplicationSource] = JToken.FromObject(Database.TransactionalStorage.Id);
        }

        private long NextId()
        {
            var incrementedCurrentLow = Interlocked.Increment(ref currentLo);
            if (incrementedCurrentLow > Capacity)
            {
                lock (generatorLock)
                {
                    if (Thread.VolatileRead(ref currentLo) > Capacity)
                    {
                        currentHi = GetNextHi();
                        currentLo = 1;
                        incrementedCurrentLow = 1;
                    }
                }
            }
            return (currentHi - 1) * Capacity + (incrementedCurrentLow);
        }

        private long GetNextHi()
        {
            while (true)
            {
                try
                {
                    var document = Database.Get(ReplicationConstants.RavenReplicationVersionHiLo, null);
                    if (document == null)
                    {
                        Database.Put(ReplicationConstants.RavenReplicationVersionHiLo,
                                     Guid.Empty,
                                     // sending empty guid means - ensure the that the document does NOT exists
                                     JObject.FromObject(new HiLoKey {ServerHi = 2}),
                                     new JObject(),
                                     null);
                        return 1;
                    }
                    var hiLoKey = document.DataAsJson.JsonDeserialization<HiLoKey>();
                    var newHi = hiLoKey.ServerHi;
                    hiLoKey.ServerHi += 1;
                    Database.Put(ReplicationConstants.RavenReplicationVersionHiLo, document.Etag,
                                 JObject.FromObject(hiLoKey),
                                 document.Metadata, null);
                    return newHi;
                }
                catch (ConcurrencyException)
                {
                    // expected, we need to retry
                }
            }
        }


        private class HiLoKey
        {
            public long ServerHi { get; set; }

        }

    }
}
