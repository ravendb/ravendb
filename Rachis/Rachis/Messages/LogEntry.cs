using System;
using System.IO;

namespace Rachis.Messages
{
    public class LogEntry
    {
        public long Index { get; set; }
        public long Term { get; set; }
        public bool? IsTopologyChange { get; set; }
        public byte[] Data { get; set; }

        public static LogEntry ReadFromStream(Stream stream)
        {
            var logEntry = new LogEntry
            {
                Index = Read7BitEncodedInt(stream),
                Term = Read7BitEncodedInt(stream),
                IsTopologyChange = stream.ReadByte() == 1
            };

            var lengthOfData = (int) Read7BitEncodedInt(stream);
            logEntry.Data = new byte[lengthOfData];

            var start = 0;
            while (start < lengthOfData)
            {
                var read = stream.Read(logEntry.Data, start, lengthOfData - start);
                start += read;
            }

            return logEntry;
        }

        public void WriteToStream(Stream stream)
        {
            Write7BitEncodedInt64(stream, Index);
            Write7BitEncodedInt64(stream, Term);
            stream.WriteByte(IsTopologyChange == true ? (byte)1 : (byte)0);
            Write7BitEncodedInt64(stream, Data.Length);
            stream.Write(Data, 0, Data.Length);            
        }

        private static long Read7BitEncodedInt(Stream stream)
        {
            long count = 0;
            int shift = 0;
            byte b;
            do
            {
                if (shift == 9 * 7)
                    throw new InvalidDataException("Invalid 7bit shifted value, used more than 9 bytes");

                var maybeEof = stream.ReadByte();
                if (maybeEof == -1)
                    throw new EndOfStreamException();

                b = (byte)maybeEof;
                count |= (uint)(b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
            return count;
        }

        private static void Write7BitEncodedInt64(Stream stream, long value)
        {
            var v = (ulong)value;
            while (v >= 128)
            {
                stream.WriteByte((byte)(v | 128));
                v >>= 7;
            }
            stream.WriteByte((byte)(v));
        }

        public override string ToString()
        {
            return $"Index={Index}, Term={Term} IsTopologyChange={IsTopologyChange ?? false}{Environment.NewLine}Data={Convert.ToBase64String(Data)}";
        }
    }
}
