using System;
using System.Diagnostics;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;

namespace Raven.Server.Documents.TimeSeries
{
    public unsafe struct BitsBuffer
    {
        private byte* _buffer;
#if DEBUG
        // This is used for range checks in debug
        public Span<byte> Buffer
        {
            get => new Span<byte>(_buffer, Size - sizeof(BitsBufferHeader) - Header->CompressedSize);
        }
#else
        public byte* Buffer => _buffer;
#endif

        public int Size;
        public BitsBufferHeader* Header;

        public int NumberOfBits => Header->UncompressedBitsPosition + Header->UncompressedSize * 8;

        public int NumberOfBytes => UncompressedBitsInBytes + Header->CompressedSize + sizeof(BitsBufferHeader);

        private int UncompressedBitsInBytes
        {
            get
            {
                var bits = Header->UncompressedBitsPosition;
                var rawBytes = bits / 8 + (bits % 8 == 0 ? 0 : 1) ;
                return rawBytes;
            }
        }

        public bool IsCompressed => Header->CompressedSize > 0;

        public bool EnsureAdditionalBits(ByteStringContext allocator, int numberOfBits)
        {
            if (HasEnoughBits(Header->UncompressedBitsPosition, numberOfBits))
                return true;

            return TryCompressBuffer(allocator, numberOfBits / 8);
        }

        public bool TryCompressBuffer(ByteStringContext allocator, int requiredBytes)
        {
            // we explicitly don't handle the remaining bits here, we only copy fully usable range
            var bytesCopied = Header->UncompressedBitsPosition / 8;

            var totalUncompressedSize = Header->UncompressedSize + bytesCopied;
            if (totalUncompressedSize >= ushort.MaxValue)
                return false; // we don't allow compressed segments whose uncompressed size is > 64Kb

            var compressedBufferPtr = ((byte*)Header) + sizeof(BitsBufferHeader);

            using (allocator.Allocate(totalUncompressedSize, out var uncompressedBuffer))
            using (allocator.Allocate(LZ4.MaximumOutputLength(totalUncompressedSize), out var newCompressionBuffer))
            {
                if (Header->CompressedSize > 0)
                {
                    LZ4.Decode64(
                        compressedBufferPtr, Header->CompressedSize,
                        uncompressedBuffer.Ptr, Header->UncompressedSize,
                        knownOutputLength: true);// will throw if not equal
                }

                Memory.Copy(uncompressedBuffer.Ptr + Header->UncompressedSize, _buffer,
                   bytesCopied);

                var len = LZ4.Encode64(
                    uncompressedBuffer.Ptr, newCompressionBuffer.Ptr,
                    uncompressedBuffer.Length, newCompressionBuffer.Length);

                if (len >= Size - sizeof(BitsBufferHeader) - 1 /*last byte bits*/ - requiredBytes || // doesn't give us enough
                    len >= ushort.MaxValue) // just to be safe, Size should always be smaller anyway
                    return false;

                var copyRemainingBits = Header->UncompressedBitsPosition % 8 != 0;

                byte lastByte = default;
                if(copyRemainingBits)
                    lastByte = Buffer[Header->UncompressedBitsPosition >> 3];

                Header->CompressedSize = (ushort)len;
                Header->UncompressedSize = (ushort)totalUncompressedSize;
                Header->UncompressedBitsPosition %= 8;


                Memory.Copy(compressedBufferPtr, newCompressionBuffer.Ptr, len);
                _buffer = compressedBufferPtr + len;
                // clear the old data
                Memory.Set(_buffer, 0, Size - sizeof(BitsBufferHeader) - Header->CompressedSize);
                if (copyRemainingBits)
                    Buffer[0] = lastByte;

                return true;
            }
        }

        public bool HasEnoughBits(int bitsPosition, int numberOfBits)
        {
            return bitsPosition + numberOfBits <= (Size - sizeof(BitsBufferHeader) - Header->CompressedSize) * 8;
        }


        public BitsBuffer(byte* buffer, int size)
        {
            Header = (BitsBufferHeader*)buffer;
            _buffer = buffer + sizeof(BitsBufferHeader) + Header->CompressedSize;
            Size = size;
        }


        private ushort BitsAvailableInLastByte()
        {
            var numBits = NumberOfBits;
            if (numBits == 0)
                return 8;
            int bitsAvailable = ((numBits & 0x7) != 0) ? (8 - (numBits & 0x7)) : 0;
            return (ushort)bitsAvailable;
        }

        public int FindTheFirstZeroBit(ref int bitsPosition, int limit)
        {
            int bits = 0;
            while (bits < limit)
            {
                var bit = ReadValue(ref bitsPosition, 1);
                if (bit == 0)
                {
                    return bits;
                }

                ++bits;
            }

            return bits;
        }


        public ulong ReadValue(ref int bitsPosition, int bitsToRead)
        {
            if(Header->CompressedSize != 0)
                throw new ArgumentException($"Cannot read from a compressed bits buffer");

            if (bitsToRead > 64)
                throw new ArgumentException($"Unable to read more than 64 bits at a time.  Requested {bitsToRead} bits", nameof(bitsToRead));

            if (bitsPosition + bitsToRead > Size * 8)
                throw new ArgumentException($"Not enough bits left in the buffer. Requested {bitsToRead} bits.  Current Position: {bitsPosition}", nameof(bitsToRead));

            ulong value = 0;
            for (int i = 0; i < bitsToRead; i++)
            {
                value <<= 1;
                ulong bit = (ulong)((Buffer[bitsPosition >> 3] >> (7 - (bitsPosition & 0x7))) & 1);
                value += bit;
                bitsPosition++;
            }

            return value;
        }

        public void SetBits(int bitsPosition, ulong value, int bitsInValue)
        {
            Debug.Assert(HasEnoughBits(bitsPosition, bitsInValue));
            if (bitsInValue == 0)
                return; // noop

            for (int i = 0; i < bitsInValue; i++)
            {
                var bit = (value & (1UL << (bitsInValue-i-1))) != 0;
                var bitIndex = bitsPosition + i;
                if (bit)
                {
                    Buffer[bitIndex >> 3] |= (byte)(1 << (7 - (bitIndex & 0x7)));
                }
                else
                {
                    Buffer[bitIndex >> 3] &= (byte)~(1 << (7 - (bitIndex & 0x7)));
                }
            }
        }

        public void AddValue(ulong value, int bitsInValue)
        {
            Debug.Assert(EnsureAdditionalBits(null, bitsInValue));

            if (bitsInValue == 0)
            {
                // Nothing to do.
                return;
            }

            var lastByteIndex = Header->UncompressedBitsPosition / 8;
            var bitsAvailable = BitsAvailableInLastByte();

            Header->UncompressedBitsPosition += (ushort)bitsInValue;

            WriteBits(value, bitsInValue, lastByteIndex, bitsAvailable);
        }

        private void WriteBits(ulong value, int bitsInValue, int lastByteIndex, ushort bitsAvailable)
        {
            if (bitsInValue <= bitsAvailable)
            {
                // The value fits in the last byte
                
                Buffer[lastByteIndex] += (byte)(value << (bitsAvailable - bitsInValue));
                return;
            }

            var bitsLeft = bitsInValue;
            if (bitsAvailable > 0)
            {
                // Fill up the last byte
                Buffer[lastByteIndex] += (byte)(value >> (bitsInValue - bitsAvailable));
                bitsLeft -= bitsAvailable;
                lastByteIndex++;
            }

            while (bitsLeft >= 8)
            {
                // We have enough bits to fill up an entire byte
                byte next = (byte)((value >> (bitsLeft - 8)) & 0xFF);
                Buffer[lastByteIndex++] = next;
                bitsLeft -= 8;
            }

            if (bitsLeft != 0)
            {
                // Start a new byte with the rest of the bits
                ulong mask = (ulong)((1 << bitsLeft) - 1L);
                byte next = (byte)((value & mask) << (8 - bitsLeft));
                Buffer[lastByteIndex] |= next;
            }
        }

        internal bool AddBits(ByteStringContext allocator, BitsBuffer tempBitsBuffer)
        {
            if (EnsureAdditionalBits(allocator, tempBitsBuffer.NumberOfBits) == false)
                return false;

            int read = 0;
            while (read < tempBitsBuffer.NumberOfBits)
            {
                var toRead = Math.Min(64, tempBitsBuffer.NumberOfBits - read);
                var result = tempBitsBuffer.ReadValue(ref read, toRead);
                AddValue(result, toRead);
            }

            return true;
        }

        internal ByteStringContext.InternalScope Uncompress(ByteStringContext allocator, out BitsBuffer bitsBuffer)
        {
            var size = Header->UncompressedSize + UncompressedBitsInBytes + sizeof(BitsBufferHeader);
            ByteStringContext.InternalScope scope = allocator.Allocate(size, out var buffer);
            Memory.Set(buffer.Ptr, 0, buffer.Length);
            if (Header->CompressedSize > 0)
            {
                var compressedBufferPtr = ((byte*)Header) + sizeof(BitsBufferHeader);

                LZ4.Decode64(compressedBufferPtr, Header->CompressedSize,
                buffer.Ptr + sizeof(BitsBufferHeader), Header->UncompressedSize,
                knownOutputLength: true);
            }
            Memory.Copy(buffer.Ptr + sizeof(BitsBufferHeader) + Header->UncompressedSize, _buffer, UncompressedBitsInBytes);

            var bufferHeader = (BitsBufferHeader*)buffer.Ptr;
            bufferHeader->UncompressedBitsPosition = (Header->UncompressedBitsPosition + Header->UncompressedSize * 8);

            bitsBuffer = new BitsBuffer(buffer.Ptr, buffer.Length);

            return scope;
        }
    }
}
