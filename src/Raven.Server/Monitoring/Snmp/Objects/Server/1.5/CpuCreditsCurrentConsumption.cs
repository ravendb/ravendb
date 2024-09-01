using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class CpuCreditsCurrentConsumption(RavenServer.CpuCreditsState state)
        : ScalarObjectBase<OctetString>(SnmpOids.Server.CpuCreditsCurrentConsumption), IMetricInstrument<double>
    {
        protected override OctetString GetData()
        {
            return new OctetString(state.CurrentConsumption.ToString("F1"));
        }

        public double GetCurrentMeasurement() => state.CurrentConsumption;
    }
}
