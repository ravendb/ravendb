using System.Globalization;
using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class CpuCreditsFailoverAlertRaised : ScalarObjectBase<OctetString>
    {
        private readonly RavenServer.CpuCreditsState _state;

        public CpuCreditsFailoverAlertRaised(RavenServer.CpuCreditsState state)
            : base(SnmpOids.Server.CpuCreditsFailoverAlertRaised)
        {
            _state = state;
        }

        protected override OctetString GetData()
        {
            return new OctetString(_state.FailoverAlertRaised.IsRaised().ToString(CultureInfo.InvariantCulture));
        }
    }
}
