using System;
using Sparrow.Compression;

namespace Sparrow.Json
{
    public unsafe class LazyCompressedStringValue 
    {
        private readonly JsonOperationContext _context;
        public readonly byte* Buffer;
        public readonly int UncompressedSize;
        public readonly int CompressedSize;
        public string String;

        public LazyStringValue ToLazyStringValue()
        {
            return new LazyStringValue(null, DecompressToTempBuffer(), UncompressedSize, _context);
        }

        public LazyCompressedStringValue(string str, byte* buffer, int uncompressedSize, int compressedSize, JsonOperationContext context)
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
            var tempBuffer = _context.GetNativeTempBuffer(UncompressedSize);
            int uncompressedSize;

            if (UncompressedSize > 128)
            {
                uncompressedSize = LZ4.Decode64(Buffer,
                    CompressedSize,
                    tempBuffer,
                    UncompressedSize,
                    true);
            }
            else
            {
                uncompressedSize = SmallStringCompression.Instance.Decompress(Buffer,
                    CompressedSize,
                    tempBuffer,
                    UncompressedSize);
            }

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