using System.Threading;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ThreadPoolAvailableWorkerThreads() : ScalarObjectBase<Gauge32>(SnmpOids.Server.ThreadPoolAvailableWorkerThreads), IMetricInstrument<int>
    {
        private int Value
        {
            get
            {
                ThreadPool.GetAvailableThreads(out var workerThreads, out _);
                return workerThreads;
            }
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        public int GetCurrentMeasurement() => Value;
    }
}
