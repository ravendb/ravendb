using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class CpuCreditsMax(RavenServer.CpuCreditsState state) : ScalarObjectBase<Integer32>(SnmpOids.Server.CpuCreditsMax), IMetricInstrument<int>
    {
        private int Value => (int)state.MaxCredits;

        protected override Integer32 GetData()
        {
            return new Integer32(Value);
        }
        
        public int GetCurrentMeasurement() => Value;
    }
}
