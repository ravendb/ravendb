using System;
using System.Net;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL.Types
{
    public class PgInt2 : PgType
    {
        public static readonly PgInt2 Default = new();
        public override int Oid => PgTypeOIDs.Int2;
        public override short Size => sizeof(short);
        public override int TypeModifier { get; }

        public override ReadOnlyMemory<byte> ToBytes(object value, PgFormat formatCode)
        {
            if (formatCode == PgFormat.Text)
            {
                return Utf8GetBytes(value);
            }

            return BitConverter.GetBytes(IPAddress.HostToNetworkOrder((short)value));
        }

        public override object FromBytes(byte[] buffer, PgFormat formatCode)
        {
            if (formatCode == PgFormat.Text)
            {
                return FromString(Utf8GetString(buffer));
            }

            return IPAddress.NetworkToHostOrder(BitConverter.ToInt16(buffer));
        }

        public override object FromString(string value)
        {
            return short.Parse(value);
        }
    }
}
