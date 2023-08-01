using System;

namespace Raven.Server.Monitoring.Snmp
{

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public sealed class SnmpIndexAttribute : Attribute
    {
        public int Index { get; }

        public SnmpIndexAttribute(int index)
        {
            Index = index;
        }
    }
}
