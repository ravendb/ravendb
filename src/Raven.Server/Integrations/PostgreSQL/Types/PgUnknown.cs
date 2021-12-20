using System;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL.Types
{
    public class PgUnknown : PgType
    {
        public static readonly PgUnknown Default = new();
        public override int Oid => PgTypeOIDs.Unknown;
        public override short Size => -1;
        public override int TypeModifier => -1;

        public override ReadOnlyMemory<byte> ToBytes(object value, PgFormat formatCode)
        {
            return Utf8GetBytes(value);
        }

        public override object FromBytes(byte[] buffer, PgFormat formatCode)
        {
            return buffer;
        }

        public override object FromString(string value)
        {
            return value;
        }
    }
}
