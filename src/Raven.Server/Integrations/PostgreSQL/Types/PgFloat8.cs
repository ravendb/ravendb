using System;
using System.Linq;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL.Types
{
    public class PgFloat8 : PgType
    {
        public static readonly PgFloat8 Default = new();
        public override int Oid => PgTypeOIDs.Float8;
        public override short Size => sizeof(double);
        public override int TypeModifier => -1;

        public override ReadOnlyMemory<byte> ToBytes(object value, PgFormat formatCode)
        {
            if (formatCode == PgFormat.Text)
            {
                return Utf8GetBytes(value);
            }

            return BitConverter.GetBytes((double)value).Reverse().ToArray();
        }

        public override object FromBytes(byte[] buffer, PgFormat formatCode)
        {
            if (formatCode == PgFormat.Text)
            {
                return FromString(Utf8GetString(buffer));
            }

            return BitConverter.ToDouble(buffer.Reverse().ToArray());
        }

        public override object FromString(string value)
        {
            return double.Parse(value);
        }
    }
}
