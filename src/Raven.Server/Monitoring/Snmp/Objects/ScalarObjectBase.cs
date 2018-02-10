using System;
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
                    return DefaultValue();

                return data;
            }

            set => throw new AccessFailureException();
        }

        protected ISnmpData DefaultValue()
        {
            var type = typeof(TData);
            if (type == typeof(OctetString))
                return DefaultOctetString;

            if (type == typeof(Integer32))
                return DefaultInteger32;

            if (type == typeof(Gauge32))
                return DefaultGauge32;

            if (type == typeof(TimeTicks))
                return DefaultTimeTicks;

            throw new NotSupportedException(type.ToString());
        }

        private static readonly TimeTicks DefaultTimeTicks = new TimeTicks(0);

        private static readonly Gauge32 DefaultGauge32 = new Gauge32(0);

        private static readonly Integer32 DefaultInteger32 = new Integer32(0);

        protected static readonly OctetString DefaultOctetString = new OctetString("N/A");
    }
}
