using System;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Json;
using Raven.Http.Exceptions;

namespace Raven.Bundles.Replication.Triggers
{
    internal class ReplicationHiLo
    {
        private long currentLo = Capacity + 1;
        private readonly object generatorLock = new object();
        private long currentHi;
        private const long Capacity = 1024 * 16;

        public DocumentDatabase Database { get; set; }
        
        public long NextId()
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
                                     JObject.FromObject(new HiLoKey { ServerHi = 2 }),
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