using System;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL.Types
{
    public class PgChar : PgType
    {
        public static readonly PgChar Default = new();
        public override int Oid => PgTypeOIDs.Char;
        public override short Size => sizeof(byte);
        public override int TypeModifier => -1;

        public override ReadOnlyMemory<byte> ToBytes(object value, PgFormat formatCode)
        {
            return Utf8GetBytes(value); // TODO: Verify this works
        }

        public override object FromBytes(byte[] buffer, PgFormat formatCode)
        {
            return Utf8GetString(buffer); // TODO: Verify this works
        }

        public override object FromString(string value)
        {
            return value;
        }
    }
}
