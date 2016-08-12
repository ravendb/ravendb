using System;
using System.Diagnostics;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Data;

namespace Raven.Client.Document.SessionOperations
{
    public class ShardLoadOperation : LoadOperation
    {

        public ShardLoadOperation(InMemoryDocumentSessionOperations sessionOperations, Func<IDisposable> disableAllCaching, string id) : base(sessionOperations, disableAllCaching, id)
        {
        }

        public override T Complete<T>()
        {
            if (documentFound == null)
            {
                // We don't register this document this as missing since it can exist in different shard 
                // but we could not use PotentialShardFor 
                // sessionOperations.RegisterMissing(id);
                return default(T);
            }
            return sessionOperations.TrackEntity<T>(documentFound);
        }

    }
}
