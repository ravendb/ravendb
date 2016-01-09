using System;
using Voron.Util;

namespace Raven.Server.Json
{
    public unsafe class LazyCompressedStringValue 
    {
        private readonly RavenOperationContext _context;
        public readonly byte* Buffer;
        public readonly int UncompressedSize;
        public readonly int CompressedSize;
        public string String;

        public LazyCompressedStringValue(string str, byte* buffer, int uncompressedSize, int compressedSize, RavenOperationContext context)
        {
            String = str;
            UncompressedSize = uncompressedSize;
            CompressedSize = compressedSize;
            _context = context;
            Buffer = buffer;
        }

        public static implicit operator string(LazyCompressedStringValue self)
        {
            if (self.String != null)
                return self.String;

            var tempBuffer = self.DecompressToTempBuffer();

            var charCount = self._context.Encoding.GetCharCount(tempBuffer, self.UncompressedSize);
            var str = new string(' ', charCount);
            fixed (char* pStr = str)
            {
                self._context.Encoding.GetChars(tempBuffer, self.UncompressedSize, pStr, charCount);
                self.String = str;
                return str;
            }
        }

        public byte* DecompressToTempBuffer()
        {
            int bufferSize;
            var tempBuffer = _context.GetNativeTempBuffer(UncompressedSize, out bufferSize);
            var uncompressedSize = LZ4.Decode64(Buffer,
                CompressedSize,
                tempBuffer,
                UncompressedSize,
                true);

            if (uncompressedSize != UncompressedSize)
                throw new FormatException("Wrong size detected on decompression");
            return tempBuffer;
        }

        public override string ToString()
        {
            return (string) this;
        }
    }
}