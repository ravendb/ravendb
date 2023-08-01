using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class CpuCreditsMax : ScalarObjectBase<Integer32>
    {
        private readonly RavenServer.CpuCreditsState _state;

        public CpuCreditsMax(RavenServer.CpuCreditsState state) 
            : base(SnmpOids.Server.CpuCreditsMax)
        {
            _state = state;
        }

        protected override Integer32 GetData()
        {
            return new Integer32((int)_state.MaxCredits);
        }
    }
}
