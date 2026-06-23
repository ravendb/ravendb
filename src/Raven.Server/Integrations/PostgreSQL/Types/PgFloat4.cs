using System;
using System.Globalization;
using System.Linq;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL.Types
{
    public sealed class PgFloat4 : PgType
    {
        public static readonly PgFloat4 Default = new();
        public override int Oid => PgTypeOIDs.Float4;
        public override short Size => sizeof(float);
        public override int TypeModifier => -1;

        public override ReadOnlyMemory<byte> ToBytes(object value, PgFormat formatCode)
        {
            if (formatCode == PgFormat.Text)
            {
                return Utf8GetBytes(((float)value).ToString(CultureInfo.InvariantCulture));
            }

            return Enumerable.Reverse(BitConverter.GetBytes((float)value)).ToArray();
        }

        public override object FromBytes(byte[] buffer, PgFormat formatCode)
        {
            if (formatCode == PgFormat.Text)
            {
                return FromString(Utf8GetString(buffer));
            }

            return BitConverter.ToSingle(Enumerable.Reverse(buffer).ToArray());
        }

        public override object FromString(string value)
        {
            return float.Parse(value, NumberStyles.Float, CultureInfo.InvariantCulture);
        }
    }
}
