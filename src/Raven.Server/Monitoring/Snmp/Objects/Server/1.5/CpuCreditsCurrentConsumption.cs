using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class CpuCreditsCurrentConsumption : ScalarObjectBase<OctetString>
    {
        private readonly RavenServer.CpuCreditsState _state;

        public CpuCreditsCurrentConsumption(RavenServer.CpuCreditsState state) 
            : base(SnmpOids.Server.CpuCreditsCurrentConsumption)
        {
            _state = state;
        }

        protected override OctetString GetData()
        {
            return new OctetString(_state.CurrentConsumption.ToString("F1"));
        }
    }
}
