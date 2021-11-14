using System;
using System.Net;
using System.Runtime.InteropServices;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL.Types
{
    public class PgInterval : PgType
    {
        public static readonly PgInterval Default = new();
        public override int Oid => PgTypeOIDs.Interval;
        public override short Size => 16;
        public override int TypeModifier => -1;

        public override ReadOnlyMemory<byte> ToBytes(object value, PgFormat formatCode)
        {
            if (formatCode == PgFormat.Text)
            {
                return Utf8GetBytes(value);
            }

            var ts = (TimeSpan)value;
            var arr = new byte[sizeof(long) + sizeof(int) + sizeof(int)];

            var ticksBuf = BitConverter.GetBytes(IPAddress.HostToNetworkOrder((ts.Ticks - ts.Days * TimeSpan.TicksPerDay) / PgTimestamp.TicksMultiplier));
            var daysBuf = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(ts.Days));
            var monthsBuf = BitConverter.GetBytes(0);

            ticksBuf.CopyTo(arr, 0);
            daysBuf.CopyTo(arr, sizeof(long));
            monthsBuf.CopyTo(arr, sizeof(long) + sizeof(int));

            return arr;
        }

        public override object FromBytes(byte[] buffer, PgFormat formatCode)
        {
            if (formatCode == PgFormat.Text)
            {
                return FromString(Utf8GetString(buffer)); // TODO: Verify this works
            }

            return GetTimeSpan(buffer);
        }

        public override object FromString(string value)
        {
            return TimeSpan.Parse(value);
        }

        private static object GetTimeSpan(byte[] buffer)
        {
            var pos = 0;
            var spanView = new ReadOnlySpan<byte>(buffer);

            var ticks = IPAddress.NetworkToHostOrder(MemoryMarshal.AsRef<long>(spanView));
            pos += sizeof(long);

            var days = IPAddress.NetworkToHostOrder(MemoryMarshal.AsRef<int>(spanView[pos..]));
            pos += sizeof(int);

            var months = IPAddress.NetworkToHostOrder(MemoryMarshal.AsRef<int>(spanView[pos..]));
            pos += sizeof(int);

            return new TimeSpan((ticks * PgTimestamp.TicksMultiplier) + (days * TimeSpan.TicksPerDay));
        }
    }
}
