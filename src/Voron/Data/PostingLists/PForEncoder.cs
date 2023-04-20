using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using Sparrow.Compression;

namespace Voron.Data.PostingLists
{
    
    /*
   * See Binary Packing (2.6) here: https://arxiv.org/pdf/1209.2137.pdf
   * for the baseline idea on the format. 
   * 
   * The output represent a bit stream with format that is a sequence of header | data pairs.
   * The header is defined as: 
   * 
   * * 0b00  (9 bits) - fixed header marker - followed by 3 bits value indicating the number of numbers following
   *                  * 0b00 - 1 value
   *                  * 0b01 - 32 values
   *                  * 0b10 - 64 values
   *                  * 0b11 - 128 values
   *            then 5 bits value (B bits per number)
   * * 0b01   (15 bits) - variable header marker - followed by 8 bits value (number of items) and 5 bits value (B bits per number)
   * * 0b10   (15 bits) - repeated header marker - followed by 8 bits value (number of repetitions) and 5 bits value (B bits per number) 
   * * 0b11   Extended value, need to read another 2 bits to understand
   * *  - 0b00 - End of entries
   * *  - 0b01 - Reserved  
   * *  - 0b10 - Raw value base for future entries - 64 bits
   * *  - 0b11 - Reserved
   */
    public unsafe ref struct PForEncoder
    {
        public const int BufferLen = 128;

        private Span<byte> _output;
        private int _bufPos, _bitPos;
        private readonly int _maxNumOfBits;
        private readonly uint* _deltasBuffer;
        public long Last;
        public long First;
        public int NumberOfAdditions;
        public int SizeInBytes;
        public int ConsumedBits => _bitPos;

        public List<long> GetDebugOutput()
        {
            return PForDecoder.GetDebugOutput(_output);
        }
        
        public PForEncoder(Span<byte> output, uint* scratchBuffer)
        {
            _output = output;
            _output[0] = 0; // do not assume the buffer is clean
            _bufPos = 0;
            _bitPos = 0;
            Debug.Assert(_output.Length > 1);
            _maxNumOfBits = (output.Length - 1) * 8; 
            Last = -1;
            First = -1;
            SizeInBytes = -1;
            NumberOfAdditions = 0;
            _deltasBuffer = scratchBuffer;
        }

        public bool TryAdd(long value)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException();
            if (NumberOfAdditions > 0 && Last > value)
                throw new ArgumentOutOfRangeException();
            if (Last == value)
                return true; // duplicate addition

            if (NumberOfAdditions++ == 0)
            {
                Last = value;
                First = value;

                return TryPushBits(0b11_10, 4) &&
                       TryPushBits((ulong)value, 63);
            }

            if (_bufPos < BufferLen)
            {
                var diff = value - Last;
                Last = value;
                if (diff >= int.MaxValue)
                {
                    return TryFlush() && 
                           TryPushBits(0b11_10, 4) &&
                           TryPushBits((ulong)value, 63);
                }

                _deltasBuffer[_bufPos++] = (uint)diff;
                return true;
            }

            NumberOfAdditions--;

            return TryFlush() && TryAdd(value);
        }

        private bool TryWriteSegmentSuffix()
        {
            byte* buf = stackalloc byte[10];
            var lenAdditions = VariableSizeEncoding.Write(buf, NumberOfAdditions);
            var lenLen = VariableSizeEncoding.Write(buf + lenAdditions, Last);
            
            var pos = _bitPos / 8;
            if (pos + lenAdditions + lenLen > _output.Length)
                return false;

            // copy the count of items & last value as reversed var-ints
            for (int i = lenAdditions - 1; i >= 0; i--)
            {
                _output[pos++] = buf[i];
            }
            for (int i = lenAdditions + lenLen - 1; i >= lenAdditions; i--)
            {
                _output[pos++] = buf[i];
            }
            
            SizeInBytes = pos;
            _bitPos = int.MaxValue;
            return true;
        }

        public bool TryCloseWithSuffix()
        {
            return TryCloseInternal() && TryWriteSegmentSuffix();
        }

        public bool TryClose()
        {
            var result = TryCloseInternal();
            SizeInBytes = _bitPos / 8 + (_bitPos % 8 == 0 ? 0 : 1);
            _bitPos = int.MaxValue;
            return result;
        }

        private bool TryCloseInternal()
        {
            if (TryFlush() == false ||
                TryPushBits(0b11_00, 4) == false)
                return false;

            var bitsToAlign = BitsAvailableInCurrentByte;
            return bitsToAlign == 0 ||
                TryPushBits(0, BitsAvailableInCurrentByte);// align to byte boundary
        }

        private bool TryFlush(uint* buffer, int len)
        {
            if (len == 0)
                return true;
            if (len == 1)
            {
                var bits = 32 - BitOperations.LeadingZeroCount(buffer[0]);
                Debug.Assert(bits < 32); // we never encode 0
                ulong header = 0b00_00_00000ul | (uint)bits;
                return TryPushBits(header, 9) && TryPushBits(buffer[0], bits);
            }

            var (maxBits, identicalPrefix) = Analyze(buffer, len);
            Debug.Assert(identicalPrefix <= 256);
            if (identicalPrefix > 5) // enough to warrant a repeating header to save space
            {
                ulong header = 0b10_00000000_00000ul | (uint)identicalPrefix << 5 | (uint)maxBits;
                if (TryPushBits(header, 15) == false ||
                    TryPushBits(buffer[0], maxBits) == false)
                    return false;
                return TryFlush(buffer + identicalPrefix, len - identicalPrefix);
            }

            ulong fixedSizeMarker = len switch
            {
                32 => 0b01,
                64 => 0b10,
                128 => 0b11,
                _ => 0
            };
            if (fixedSizeMarker != 0)
            {
                if (TryPartialCompression(buffer, len))
                    return true;

                ulong header = 0b00_00_00000ul | fixedSizeMarker << 5 | (uint)maxBits;
                if (TryPushBits(header, 9) == false)
                    return false;
                return TryPushValues(buffer, len, maxBits);
            }
            else
            {
                if (TryPartialCompression(buffer, len))
                    return true;
                ulong header = 0b01_0000_0000_00000ul | (ulong)len << 5 | (uint)maxBits;
                if (TryPushBits(header, 15) == false)
                    return false;
                return TryPushValues(buffer, len, maxBits);
            }
        }

        private bool TryPartialCompression(uint* buffer, int len)
        {
            var half = len / 2;
            if (half < 32)
                return false;

            // now need to figure out optimal partitioning scheme
            int first = MaxBits(buffer, half), second = MaxBits(buffer + half, len - half);

            if (first != second) // better to output them as two segments, then
            {
                return TryFlush(buffer, half) &&
                       TryFlush(buffer + half, len - half);
            }
            return false;
        }


        private bool TryPushValues(uint* buffer, int len, int maxBits)
        {
            for (int i = 0; i < len; i++)
            {
                if (TryPushBits(buffer[i], maxBits) == false)
                {
                    return false;
                }
            }

            return true;
        }

        private bool TryFlush()
        {
            var oldPos = _bufPos;
            _bufPos = 0;
            return TryFlush(_deltasBuffer, oldPos);
        }

        private static int MaxBits(uint* buffer, int len)
        {
            uint mask = 0;
            for (int i = 0; i < len; i++)
            {
                mask |= buffer[i];
            }

            var maxBits = 32 - BitOperations.LeadingZeroCount(mask);
            Debug.Assert(maxBits < 32); // we never encode 0
            return maxBits;
        }

        private static (int MaxNumberOfBits, int IdenticalPrefixLegnth) Analyze(uint* buffer, int len)
        {
            uint mask = buffer[0];
            int identicalPrefix = 1;
            bool identical = true;
            for (int i = 1; i < len; i++)
            {
                mask |= buffer[i];
                if (identical)
                {
                    if (buffer[i] == buffer[0])
                    {
                        identicalPrefix++;
                    }
                    else
                    {
                        identical = false;
                    }
                }
            }

            var maxBits = 32 - BitOperations.LeadingZeroCount(mask);
            Debug.Assert(maxBits < 32); // we never encode 0
            return (maxBits, identicalPrefix);
        }

        // see: 
        // https://github.com/facebookarchive/beringei/blob/75c3002b179d99c8709323d605e7d4b53484035c/beringei/lib/BitUtil.cpp#L17
        public bool TryPushBits(ulong value, int bitsInValue)
        {
            Debug.Assert(bitsInValue > 0);

            if (_bitPos + bitsInValue > _maxNumOfBits)
            {
                return false;
            }

            int bitsAvailable = BitsAvailableInCurrentByte;
            var bytePos = _bitPos / 8;
            _bitPos += bitsInValue;
            if (bitsInValue <= bitsAvailable)
            {
                // Everything fits inside the last byte
                _output[bytePos] += (byte)(value << bitsAvailable - bitsInValue);
                return true;
            }

            int bitsLeft = bitsInValue;
            if (bitsAvailable > 0)
            {
                // Fill up the last byte
                _output[bytePos] += (byte)(value >> bitsInValue - bitsAvailable);
                bitsLeft -= bitsAvailable;
                bytePos++;
            }

            while (bitsLeft >= 8)
            {
                // Enough bits for a dedicated byte
                _output[bytePos] = (byte)(value >> bitsLeft - 8 & 0xFF);
                bitsLeft -= 8;
                bytePos++;
            }

            if (bitsLeft != 0)
            {
                // Start a new byte with the rest of the bits
                _output[bytePos] = (byte)((value & (1U << bitsLeft) - 1) << 8 - bitsLeft);
            }

            return true;
        }


        private int BitsAvailableInCurrentByte
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            get => (_bitPos & 0x7) != 0 ? 8 - (_bitPos & 0x7) : 0;
        }
    }
}
