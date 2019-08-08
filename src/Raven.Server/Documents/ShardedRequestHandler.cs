using System;
using System.Runtime.InteropServices;
using Raven.Server.Web;

namespace Raven.Server.Documents
{
    public class ShardedRequestHandler : RequestHandler
    {
        protected static ushort GetShardId(string key)
        {
            return Crc16.ComputeChecksum(MemoryMarshal.AsBytes(key.AsSpan()));
        }

        public static class Crc16
        {
            const ushort Polynomial = 0xA001;
            static readonly ushort[] Table = new ushort[256];

            public static ushort ComputeChecksum(ReadOnlySpan<byte> bytes)
            {
                ushort crc = 0;
                for (int i = 0; i < bytes.Length; ++i)
                {
                    byte index = (byte)(crc ^ bytes[i]);
                    crc = (ushort)((crc >> 8) ^ Table[index]);
                }
                return crc;
            }

            static Crc16()
            {
                for (ushort i = 0; i < Table.Length; ++i)
                {
                    ushort value = 0;
                    var temp = i;
                    for (byte j = 0; j < 8; ++j)
                    {
                        if (((value ^ temp) & 0x0001) != 0)
                        {
                            value = (ushort)((value >> 1) ^ Polynomial);
                        }
                        else
                        {
                            value >>= 1;
                        }
                        temp >>= 1;
                    }
                    Table[i] = value;
                }
            }
        }
    }
}
