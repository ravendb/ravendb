using System.Threading;
using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ThreadPoolAvailableCompletionPortThreads : ScalarObjectBase<Gauge32>
    {
        public ThreadPoolAvailableCompletionPortThreads()
            : base(SnmpOids.Server.ThreadPoolAvailableCompletionPortThreads)
        {
        }

        protected override Gauge32 GetData()
        {
            ThreadPool.GetAvailableThreads(out _, out var completionPortThreeads);
            return new Gauge32(completionPortThreeads);
        }
    }
}
