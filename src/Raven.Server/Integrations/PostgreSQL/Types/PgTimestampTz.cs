using System;
using System.Net;
using System.Text;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL.Types
{
    public class PgTimestampTz : PgType
    {
        public static readonly PgTimestampTz Default = new();
        public override int Oid => PgTypeOIDs.TimestampTz;
        public override short Size => 8;
        public override int TypeModifier => -1;

        public override ReadOnlyMemory<byte> ToBytes(object value, PgFormat formatCode)
        {
            switch (value)
            {
                case DateTime dateTimeValue:
                    if (formatCode == PgFormat.Text)
                        return Encoding.UTF8.GetBytes(dateTimeValue.ToString("yyyy-MM-dd HH:mm:ss.fffffffzz"));

                    return BitConverter.GetBytes(IPAddress.HostToNetworkOrder(GetTimestampTz(dateTimeValue)));
                case DateTimeOffset dateTimeOffsetValue:
                    if (formatCode == PgFormat.Text)
                        return Encoding.UTF8.GetBytes(dateTimeOffsetValue.ToString("yyyy-MM-dd HH:mm:ss.fffffffzz"));

                    return BitConverter.GetBytes(IPAddress.HostToNetworkOrder(GetTimestampTz(dateTimeOffsetValue)));
                default:
                    throw new ArgumentOutOfRangeException(nameof(value), $"Unable to convert {value} to {nameof(PgTimestampTz)} type");
            }
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
            return DateTimeOffset.Parse(value).ToOffset(TimeSpan.Zero);
        }

        private static DateTime GetDateTime(long timestamp)
        {
            return new DateTime(timestamp * PgTimestamp.TicksMultiplier + PgTimestamp.OffsetTicks, DateTimeKind.Utc);
        }

        private static long GetTimestampTz(DateTimeOffset timestamp)
        {
            return (timestamp.Ticks - PgTimestamp.OffsetTicks) / PgTimestamp.TicksMultiplier;
        }
    }
}
