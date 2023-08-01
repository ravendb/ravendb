using System;

namespace Raven.Server.Monitoring.Snmp
{
    [AttributeUsage(AttributeTargets.Field)]
    public sealed class SnmpEnumIndexAttribute : Attribute
    {
        public SnmpEnumIndexAttribute(Type type)
        {
            if (type.IsEnum == false)
                throw new InvalidOperationException($"Only enums are supported, but was '{type}'.");

            Type = type;
        }

        public Type Type { get; }
    }
}
