using System;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL.Types
{
    public class PgBytea : PgType
    {
        public static readonly PgBytea Default = new();
        public override int Oid => PgTypeOIDs.Bytea;
        public override short Size => -1;
        public override int TypeModifier => -1;

        public override ReadOnlyMemory<byte> ToBytes(object value, PgFormat formatCode)
        {
            return (byte[])value;
        }

        public override object FromBytes(byte[] buffer, PgFormat formatCode)
        {
            return buffer; // TODO: Verify this works
        }

        public override object FromString(string value)
        {
            throw new NotSupportedException("Converting string to bytea is not supported. Tried converting: " + value);
        }
    }
}
