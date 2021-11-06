using System;
using System.Linq;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL.Types
{
    public class PgFloat4 : PgType
    {
        public static readonly PgFloat4 Default = new();
        public override int Oid => PgTypeOIDs.Float4;
        public override short Size => sizeof(float);
        public override int TypeModifier => -1;

        public override ReadOnlyMemory<byte> ToBytes(object value, PgFormat formatCode)
        {
            if (formatCode == PgFormat.Text)
            {
                return Utf8GetBytes(value);
            }

            return BitConverter.GetBytes((float)value).Reverse().ToArray();
        }

        public override object FromBytes(byte[] buffer, PgFormat formatCode)
        {
            if (formatCode == PgFormat.Text)
            {
                return FromString(Utf8GetString(buffer));
            }

            return BitConverter.ToSingle(buffer.Reverse().ToArray());
        }

        public override object FromString(string value)
        {
            return float.Parse(value);
        }
    }
}
