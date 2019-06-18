using System.Globalization;
using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class CpuCreditsBackgroundTasksAlertRaised : ScalarObjectBase<OctetString>
    {
        private readonly RavenServer.CpuCreditsState _state;

        public CpuCreditsBackgroundTasksAlertRaised(RavenServer.CpuCreditsState state)
            : base(SnmpOids.Server.CpuCreditsBackgroundTasksAlertRaised)
        {
            _state = state;
        }

        protected override OctetString GetData()
        {
            return new OctetString(_state.BackgroundTasksAlertRaised.IsRaised().ToString(CultureInfo.InvariantCulture));
        }
    }
}
