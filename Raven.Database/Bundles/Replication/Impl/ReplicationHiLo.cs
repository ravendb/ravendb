//-----------------------------------------------------------------------
// <copyright file="ReplicationHiLo.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Client.Document;
using Raven.Database;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Impl
{
    internal class ReplicationHiLo
    {
        public static long NextId(DocumentDatabase database)
        {
            var replicationHiLo = (ReplicationHiLo)database.ExtensionsState.GetOrAdd(typeof(ReplicationHiLo).AssemblyQualifiedName, o => new ReplicationHiLo(database));
            return replicationHiLo.NextId();
        }

        private readonly object generatorLock = new object();
        private volatile Holder currentMax;
        private long capacity = 256;
        private long current;
        private DateTime lastRequestedUtc;

        private ReplicationHiLo(DocumentDatabase database)
        {
            Database = database;

            // backward compatibility, read the hilo max then delete it, storing the value in the identity val

            var document = database.Documents.Get(RavenReplicationVersionHiLo, null);
            if (document == null)
            {
                currentMax = new Holder(0);
                current = 0;
                return;
            }
            var max = document.DataAsJson.Value<long>("Max");
            currentMax = new Holder(max);
            current = max;
            GetNextMax(); // this saved the new max limit as part of its work
            database.Documents.Delete(RavenReplicationVersionHiLo, null, null);
        }

        private const string RavenReplicationHilo = "Raven/Replication/Hilo";
        private const string RavenReplicationVersionHiLo = "Raven/Replication/VersionHilo";
        public DocumentDatabase Database { get; set; }

        private class Holder
        {
            public readonly long Value;

            public Holder(long value)
            {
                Value = value;
            }
        }

        public long NextId()
        {
            long incrementedCurrent = Interlocked.Increment(ref current);
            if (incrementedCurrent <= currentMax.Value)
                return incrementedCurrent;
            lock (generatorLock)
            {
                incrementedCurrent = Interlocked.Increment(ref current);
                if (incrementedCurrent <= currentMax.Value)
                    return incrementedCurrent;
                if (current > currentMax.Value)
                {
                    currentMax = new Holder(GetNextMax());
                }
                return Interlocked.Increment(ref current);
            }
        }

        private long GetNextMax()
        {
            var span = SystemTime.UtcNow - lastRequestedUtc;
            if (span.TotalSeconds < 1)
            {
                capacity *= 2;
            }
            lastRequestedUtc = SystemTime.UtcNow;

            for (int i = 0; i < 10000; i++)
            {
                try
                {
                    using (Database.TransactionalStorage.DisableBatchNesting())
                    {
                        var minNextMax = currentMax.Value;
                        long max = 0;
                        using (Database.IdentityLock.Lock())
                        {
                            Database.TransactionalStorage.Batch(accessor =>
                            {
                                var val = accessor.General.GetNextIdentityValue(RavenReplicationHilo, 0);
                                var next = Math.Max(minNextMax, val);
                                current = next + 1;
                                max = next + capacity;
                                accessor.General.SetIdentityValue(RavenReplicationHilo, max);
                            });
                        }
                       
                        return max;
                    }
                }
                catch (ConcurrencyException)
                {
                }
            }
            throw new InvalidOperationException("Unable to generate new hilo key for the replication version");
        }
    }
}
