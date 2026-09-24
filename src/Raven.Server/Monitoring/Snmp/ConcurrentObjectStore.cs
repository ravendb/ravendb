using System.Threading;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

namespace Raven.Server.Monitoring.Snmp
{
    /// <summary>
    /// <see cref="ObjectStore"/> keeps its objects in a plain list and synchronizes nothing, while we mutate it long
    /// after the engine started serving: every database that gets loaded registers its own OIDs, and so does every
    /// index, ETL process, AI task and CDC sink discovered afterwards. A lookup that runs while one of those is being
    /// registered throws "Collection was modified; enumeration operation may not execute".
    /// </summary>
    public sealed class ConcurrentObjectStore : ObjectStore
    {
        private readonly ReaderWriterLockSlim _locker = new(LockRecursionPolicy.SupportsRecursion);

        public override void Add(ISnmpObject item)
        {
            _locker.EnterWriteLock();

            try
            {
                base.Add(item);
            }
            finally
            {
                _locker.ExitWriteLock();
            }
        }

        public override ScalarObject GetObject(ObjectIdentifier id)
        {
            _locker.EnterReadLock();

            try
            {
                return base.GetObject(id);
            }
            finally
            {
                _locker.ExitReadLock();
            }
        }

        public override ScalarObject GetNextObject(ObjectIdentifier id)
        {
            _locker.EnterReadLock();

            try
            {
                return base.GetNextObject(id);
            }
            finally
            {
                _locker.ExitReadLock();
            }
        }
    }
}
