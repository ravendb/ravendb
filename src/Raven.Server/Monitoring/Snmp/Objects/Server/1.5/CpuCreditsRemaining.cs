using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class CpuCreditsRemaining(RavenServer.CpuCreditsState state) : ScalarObjectBase<Gauge32>(SnmpOids.Server.CpuCreditsRemaining), IMetricInstrument<int>
    {
        private int Value => (int)state.RemainingCpuCredits;

        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        public int GetCurrentMeasurement() => Value;
    }
}
