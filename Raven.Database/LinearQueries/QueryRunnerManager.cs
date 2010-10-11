using System;
using System.Collections.Concurrent;
using Raven.Database.Linq;
using Raven.Database.Storage;

namespace Raven.Database.LinearQueries
{
    public class QueryRunnerManager : MarshalByRefObject
    {
        private readonly ConcurrentDictionary<string, AbstractViewGenerator> queryCache =
            new ConcurrentDictionary<string, AbstractViewGenerator>();

        public override object InitializeLifetimeService()
        {
            return null;
        }

        public int QueryCacheSize
        {
            get { return queryCache.Count; }
        }

        public IRemoteSingleQueryRunner CreateSingleQueryRunner(Type remoteStorageType, object state)
        {
            var remoteStorage = (IRemoteStorage)Activator.CreateInstance(remoteStorageType, state);
            return new SingleQueryRunner(remoteStorage, queryCache);
        }
    }
}
