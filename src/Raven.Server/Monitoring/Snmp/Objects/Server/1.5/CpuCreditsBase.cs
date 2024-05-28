using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class CpuCreditsBase(RavenServer.CpuCreditsState state) : ScalarObjectBase<Integer32>(SnmpOids.Server.CpuCreditsBase), IMetricInstrument<int>
    {
        private int Value => (int)state.BaseCredits;

        protected override Integer32 GetData()
        {
            return new Integer32(Value);
        }

        public int GetCurrentMeasurement() => Value;
    }
}
