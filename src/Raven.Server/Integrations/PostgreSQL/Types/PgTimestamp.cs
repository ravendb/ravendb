using System;
using System.Net;
using System.Text;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL.Types
{
    public class PgTimestamp : PgType
    {
        public static readonly PgTimestamp Default = new();
        
        public override int Oid => PgTypeOIDs.Timestamp;
        public override short Size => 8;
        public override int TypeModifier => -1;

        public const long OffsetTicks = 630822816000000000L;
        public const int TicksMultiplier = 10;

        public override ReadOnlyMemory<byte> ToBytes(object value, PgFormat formatCode)
        {
            if (formatCode == PgFormat.Text)
            {
                return Encoding.UTF8.GetBytes(((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss.fffffff"));
            }

            var timestamp = GetTimestamp((DateTime) value);
            return BitConverter.GetBytes(IPAddress.HostToNetworkOrder(timestamp));
        }

        public override object FromBytes(byte[] buffer, PgFormat formatCode)
        {
            if (formatCode == PgFormat.Text)
            {
                return FromString(Utf8GetString(buffer)); // TODO: Verify it works
            }

            return GetDateTime(IPAddress.NetworkToHostOrder(BitConverter.ToInt64(buffer)));
        }

        public override object FromString(string value)
        {
            return DateTime.Parse(value);
        }

        private static DateTime GetDateTime(long timestamp)
        {
            return new DateTime(timestamp * PgTimestamp.TicksMultiplier + PgTimestamp.OffsetTicks);
        }

        private static long GetTimestamp(DateTime timestamp)
        {
            return (timestamp.Ticks - OffsetTicks) / TicksMultiplier;
        }
    }
}
