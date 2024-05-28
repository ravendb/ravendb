using System.Globalization;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class CpuCreditsAlertRaised : ScalarObjectBase<OctetString>, IMetricInstrument<byte>
    {
        private readonly RavenServer.CpuCreditsState _state;

        public CpuCreditsAlertRaised(RavenServer.CpuCreditsState state)
            : base(SnmpOids.Server.CpuCreditsAlertRaised)
        {
            _state = state;
        }

        private bool Value => (_state.FailoverAlertRaised.IsRaised() || _state.BackgroundTasksAlertRaised.IsRaised());

        protected override OctetString GetData()
        {
            return new OctetString(Value.ToString(CultureInfo.InvariantCulture));
        }

        public byte GetCurrentMeasurement() => (byte)(Value ? 1 : 0);
    }
}
