using System;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL.Types
{
    public class PgText : PgType
    {
        public static readonly PgText Default = new();
        public override int Oid => PgTypeOIDs.Text;
        public override short Size => -1;
        public override int TypeModifier => -1;

        public override ReadOnlyMemory<byte> ToBytes(object value, PgFormat formatCode)
        {
            return Utf8GetBytes(value);
        }

        public override object FromBytes(byte[] buffer, PgFormat formatCode)
        {
            return Utf8GetString(buffer);
        }

        public override object FromString(string value)
        {
            return value;
        }
    }
}
