using System;
using System.Net;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL.Types
{
    public class PgOid : PgType
    {
        public static readonly PgOid Default = new();
        public override int Oid => PgTypeOIDs.Oid;
        public override short Size => sizeof(int);
        public override int TypeModifier => -1;

        public override ReadOnlyMemory<byte> ToBytes(object value, PgFormat formatCode)
        {
            if (formatCode == PgFormat.Text)
            {
                return Utf8GetBytes(value);
            }

            return BitConverter.GetBytes(IPAddress.HostToNetworkOrder((int)value));
        }

        public override object FromBytes(byte[] buffer, PgFormat formatCode)
        {
            if (formatCode == PgFormat.Text)
            {
                return FromString(Utf8GetString(buffer));
            }

            return IPAddress.NetworkToHostOrder(BitConverter.ToInt32(buffer));
        }

        public override object FromString(string value)
        {
            return int.Parse(value);
        }
    }
}
