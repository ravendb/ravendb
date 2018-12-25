using System;
using Sparrow.Compression;
using static Sparrow.Hashing;

namespace Sparrow.Json
{
    public unsafe class LazyCompressedStringValue 
    {
        private readonly JsonOperationContext _context;
        public readonly byte* Buffer;
        public readonly int UncompressedSize;
        public readonly int CompressedSize;
        public string String;

        protected bool Equals(LazyCompressedStringValue other)
        {
            var size = CompressedSize;
            if (other.CompressedSize != size)
                return false;

            return Memory.CompareInline(Buffer, other.Buffer, size) == 0;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            
            switch (obj)
            {
                case LazyCompressedStringValue lcsv:
                    return Equals(lcsv);
                case LazyStringValue lsv:
                    return lsv.Equals(ToLazyStringValue());
                case string str:
                    return str.Equals(ToString());
            }

            return false;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (int)XXHash64.Calculate(Buffer, (ulong)CompressedSize);
            }
        }

        /// <summary>
        /// Returns uncompressed data in form of LazyStringValue
        /// </summary>
        /// <returns></returns>
        public LazyStringValue ToLazyStringValue()
        {
            var allocatedUncompressedData = DecompressToAllocatedMemoryData(_context);

            var lazyStringValue = _context.AllocateStringValue(null, allocatedUncompressedData.Address, UncompressedSize);

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
                var charCount = Encodings.Utf8.GetCharCount(tempBuffer, self.UncompressedSize);
                var str = new string(' ', charCount);
                fixed (char* pStr = str)
                {
                    Encodings.Utf8.GetChars(tempBuffer, self.UncompressedSize, pStr, charCount);
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

        public byte* DecompressToTempBuffer(out AllocatedMemoryData allocatedData, JsonOperationContext externalContext = null)
        {
            var sizeOfEscapePositions = GetSizeOfEscapePositions();
            allocatedData = (externalContext??_context).GetMemory(UncompressedSize + sizeOfEscapePositions);
            return DecompressToBuffer(allocatedData.Address, sizeOfEscapePositions);
        }

        public AllocatedMemoryData DecompressToAllocatedMemoryData(JsonOperationContext externalContext)
        {
            var sizeOfEscapePositions = GetSizeOfEscapePositions();
            var allocatedBuffer = externalContext.GetMemory(UncompressedSize + sizeOfEscapePositions);
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

        private int GetSizeOfEscapePositions()
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

        public string Substring(int startIndex, int length)
        {
            return ToString().Substring(startIndex, length);
        }
    }
}
