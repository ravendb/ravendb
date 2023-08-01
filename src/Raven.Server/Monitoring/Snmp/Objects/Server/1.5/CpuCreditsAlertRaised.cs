using System.Globalization;
using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class CpuCreditsAlertRaised : ScalarObjectBase<OctetString>
    {
        private readonly RavenServer.CpuCreditsState _state;

        public CpuCreditsAlertRaised(RavenServer.CpuCreditsState state)
            : base(SnmpOids.Server.CpuCreditsAlertRaised)
        {
            _state = state;
        }

        protected override OctetString GetData()
        {
            return new OctetString((_state.FailoverAlertRaised.IsRaised() || _state.BackgroundTasksAlertRaised.IsRaised()).ToString(CultureInfo.InvariantCulture));
        }
    }
}
