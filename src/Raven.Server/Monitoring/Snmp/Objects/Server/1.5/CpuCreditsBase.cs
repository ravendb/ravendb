using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class CpuCreditsBase : ScalarObjectBase<Integer32>
    {
        private readonly RavenServer.CpuCreditsState _state;

        public CpuCreditsBase(RavenServer.CpuCreditsState state) 
            : base(SnmpOids.Server.CpuCreditsBase)
        {
            _state = state;
        }

        protected override Integer32 GetData()
        {
            return new Integer32((int)_state.BaseCredits);
        }
    }
}
