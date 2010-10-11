using System;
using Raven.Database.Data;
using Raven.Database.Indexing;

namespace Raven.Bundles.Replication
{
    public static class ReplicationContext
    {
        [ThreadStatic] private static bool _currentlyInContext;

        public static bool IsInReplicationContext
        {
            get
            {
                return _currentlyInContext;
            }
        }

        public static IDisposable Enter()
        {
            var old = _currentlyInContext;
            _currentlyInContext = true;
            return new DisposableAction(() => _currentlyInContext = old);
        }
    }
}
