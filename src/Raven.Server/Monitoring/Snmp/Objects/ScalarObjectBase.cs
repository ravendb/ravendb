using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

namespace Raven.Server.Monitoring.Snmp.Objects
{
    public abstract class ScalarObjectBase<TData> : ScalarObject
        where TData : ISnmpData
    {
        protected ScalarObjectBase(string dots, bool appendRoot = true)
            : base(appendRoot ? SnmpOids.Root + dots : dots)
        {
        }

        protected ScalarObjectBase(string dots, int index, bool appendRoot = true)
            : base(appendRoot ? SnmpOids.Root + dots : dots, index)
        {
        }

        protected abstract TData GetData();
        
        public override ISnmpData Data
        {
            get
            {
                var data = GetData();
                if (data == null)
                    return NoSuchInstance;

                return data;
            }

            set => throw new AccessFailureException();
        }

        private static readonly NoSuchInstance NoSuchInstance = new NoSuchInstance();
    }
}
