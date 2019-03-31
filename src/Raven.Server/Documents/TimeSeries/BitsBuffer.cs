using System;
using System.Diagnostics;

namespace Raven.Server.Documents.TimeSeries
{
    public unsafe struct BitsBuffer
    {
        public byte* Buffer;
        public int Size;
        public BitsBufferHeader* Header;

        public int NumberOfBits => Header->BitsPosition;

        public bool HasAdditionalBits(int numberOfBits)
        {
            return HasEnoughBits(Header->BitsPosition, numberOfBits);
        }

        public bool HasEnoughBits(int bitsPosition, int numberOfBits)
        {
            return bitsPosition + numberOfBits <= Size * 8;
        }


        public BitsBuffer(byte* buffer, int size)
        {
            Header = (BitsBufferHeader*)buffer;
            Buffer = buffer + sizeof(BitsBufferHeader);
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
            Debug.Assert(HasAdditionalBits(bitsInValue));

            if (bitsInValue == 0)
            {
                // Nothing to do.
                return;
            }

            var lastByteIndex = Header->BitsPosition / 8;
            var bitsAvailable = BitsAvailableInLastByte();

            Header->BitsPosition += (ushort)bitsInValue;

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

        internal bool AddBits(BitsBuffer tempBitsBuffer)
        {
            if (HasAdditionalBits(tempBitsBuffer.NumberOfBits) == false)
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
    }
}
