using System.Threading;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

namespace Raven.Server.Monitoring.Snmp
{
    /// <summary>
    /// <see cref="ObjectStore"/> keeps its objects in a plain list without any synchronization,
    /// but objects are added by database, index and task registration while the SNMP listener
    /// and the monitoring endpoints read them concurrently
    /// </summary>
    internal sealed class SnmpObjectStore : ObjectStore
    {
        private readonly ReaderWriterLockSlim _locker = new ReaderWriterLockSlim();

        public override void Add(ISnmpObject newObject)
        {
            _locker.EnterWriteLock();
            try
            {
                base.Add(newObject);
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
