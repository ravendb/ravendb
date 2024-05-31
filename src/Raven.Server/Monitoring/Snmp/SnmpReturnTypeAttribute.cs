using System;
using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp;

[AttributeUsage(AttributeTargets.Field)]
public sealed class SnmpDataTypeAttribute : Attribute
{
    public SnmpDataTypeAttribute(SnmpType type)
    {
        TypeCode = type;
    }

    public SnmpType TypeCode { get; }
}
