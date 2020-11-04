using System.Threading;
using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ThreadPoolAvailableWorkerThreads : ScalarObjectBase<Gauge32>
    {
        public ThreadPoolAvailableWorkerThreads()
            : base(SnmpOids.Server.ThreadPoolAvailableWorkerThreads)
        {
        }

        protected override Gauge32 GetData()
        {
            ThreadPool.GetAvailableThreads(out var workerThreads, out _);
            return new Gauge32(workerThreads);
        }
    }
}
