using System.Threading;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ThreadPoolAvailableCompletionPortThreads() : ScalarObjectBase<Gauge32>(SnmpOids.Server.ThreadPoolAvailableCompletionPortThreads), IMetricInstrument<int>
    {
        private int Value
        {
            get
            {
                ThreadPool.GetAvailableThreads(out _, out var completionPortThreeads);
                return completionPortThreeads;
            }
        }
        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        public int GetCurrentMeasurement() => Value;
    }
}
