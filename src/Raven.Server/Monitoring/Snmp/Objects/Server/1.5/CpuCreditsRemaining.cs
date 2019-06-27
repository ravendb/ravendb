using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class CpuCreditsRemaining : ScalarObjectBase<Gauge32>
    {
        private readonly RavenServer.CpuCreditsState _state;

        public CpuCreditsRemaining(RavenServer.CpuCreditsState state) 
            : base(SnmpOids.Server.CpuCreditsRemaining)
        {
            _state = state;
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32((int)_state.RemainingCpuCredits);
        }
    }
}
