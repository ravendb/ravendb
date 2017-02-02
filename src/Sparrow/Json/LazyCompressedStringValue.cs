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

        /// <summary>
        /// Returns uncompressed data in form of LazyStringValue
        /// </summary>
        /// <returns></returns>
        public LazyStringValue ToLazyStringValue()
        {
            var allocatedUncompressedData = DecompressToAllocatedMemoryData();

            var lazyStringValue = new LazyStringValue(null, allocatedUncompressedData.Address, UncompressedSize, _context);

            lazyStringValue.AllocatedMemoryData = allocatedUncompressedData;
            return lazyStringValue;
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

            AllocatedMemoryData allocated;
            var tempBuffer = self.DecompressToTempBuffer(out allocated);

            try
            {
                var charCount = self._context.Encoding.GetCharCount(tempBuffer, self.UncompressedSize);
                var str = new string(' ', charCount);
                fixed (char* pStr = str)
                {
                    self._context.Encoding.GetChars(tempBuffer, self.UncompressedSize, pStr, charCount);
                    self.String = str;
                    return str;
                }
            }
            finally
            {
                if(allocated != null) //precaution
                    self._context.ReturnMemory(allocated);
            }
        }

        public byte* DecompressToTempBuffer(out AllocatedMemoryData allocatedData)
        {
            var sizeOfEscapePositions = GetSizeOfEscapePositions();
            allocatedData = _context.GetMemory(UncompressedSize + sizeOfEscapePositions);
            return DecompressToBuffer(allocatedData.Address, sizeOfEscapePositions);
        }

        public AllocatedMemoryData DecompressToAllocatedMemoryData()
        {
            var sizeOfEscapePositions = GetSizeOfEscapePositions();
            var allocatedBuffer = _context.GetMemory(UncompressedSize + sizeOfEscapePositions);
            DecompressToBuffer(allocatedBuffer.Address, sizeOfEscapePositions);

            return allocatedBuffer;
        }

        private byte* DecompressToBuffer(byte* tempBuffer, int sizeOfEscapePositions)
        {
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

            Memory.Copy(tempBuffer + uncompressedSize, Buffer + CompressedSize, sizeOfEscapePositions);
            return tempBuffer;
        }

        private unsafe int GetSizeOfEscapePositions()
        {
            var escapeSequencePos = CompressedSize;
            var numberOfEscapeSequences = BlittableJsonReaderBase.ReadVariableSizeInt(Buffer, ref escapeSequencePos);
            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;
                BlittableJsonReaderBase.ReadVariableSizeInt(Buffer, ref escapeSequencePos);
            }

            var sizeOfEscapePositions = escapeSequencePos - CompressedSize;
            return sizeOfEscapePositions;
        }

        public override string ToString()
        {
            return (string) this;
        }
    }
}