using System.Globalization;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class CpuCreditsBackgroundTasksAlertRaised : ScalarObjectBase<OctetString>, IMetricInstrument<byte>
    {
        private readonly RavenServer.CpuCreditsState _state;

        public CpuCreditsBackgroundTasksAlertRaised(RavenServer.CpuCreditsState state)
            : base(SnmpOids.Server.CpuCreditsBackgroundTasksAlertRaised)
        {
            _state = state;
        }

        private bool Value => _state.BackgroundTasksAlertRaised.IsRaised();
        
        protected override OctetString GetData()
        {
            return new OctetString(Value.ToString(CultureInfo.InvariantCulture));
        }

        public byte GetCurrentMeasurement() => (byte)(Value ? 1 : 0);
    }
}
