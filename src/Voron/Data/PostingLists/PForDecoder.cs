using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Xml;
using static System.Runtime.Intrinsics.X86.Sse2;
using static System.Runtime.Intrinsics.X86.Avx2;
using System.Runtime.Intrinsics.X86;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;

namespace Voron.Data.PostingLists
{

    public unsafe static class PForDecoder
    {
        private static readonly UnmanagedGlobalSegment _segment;
        private static readonly int* IntShiftTable;

        static PForDecoder()
        {
            ReadOnlySpan<int> table = new int[] {
                    7, 6, 5, 4, 3, 2, 1, 0,
                    7, 6, 5, 4, 3, 2, 1, 0,
                    7, 6, 5, 4, 3, 2, 1, 0,
                    7, 6, 5, 4, 3, 2, 1, 0,
                    7, 6, 5, 4, 3, 2, 1, 0,
                    7, 6, 5, 4, 3, 2, 1, 0,
                    7, 6, 5, 4, 3, 2, 1, 0,
                    7, 6, 5, 4, 3, 2, 1, 0,
                    7, 6, 5, 4, 3, 2, 1, 0 };

            _segment = ByteStringContext.Allocator.Allocate(table.Length * sizeof(int), () => throw new OutOfMemoryException());
            IntShiftTable = (int*)_segment.Segment;
            table.CopyTo(new Span<int>(IntShiftTable, table.Length));
        }

        public struct DecoderState
        {
            public int BufferSize;
            public int MaxBits => BufferSize * 8;

            public int NumberOfReads;

            internal int _bitPos;
            internal long _prevValue;

            public DecoderState(int bufferSize)
            {
                _bitPos = 0;
                _prevValue = 0;
                NumberOfReads = 0;
                BufferSize = bufferSize;
            }
        }

        private static ReadOnlySpan<byte> NumberOfValues => new byte[] { 1, 32, 64, 128 };

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static int Decode(ref DecoderState state, in ReadOnlySpan<byte> inputBuffer, in Span<long> outputBuffer)
        {
            fixed (byte* inputBufferPtr = inputBuffer)
            fixed (long* outputBufferPtr = outputBuffer)
            {
                return Decode(ref state, inputBufferPtr, inputBuffer.Length, outputBufferPtr, outputBuffer.Length);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static int Decode(ref DecoderState state, byte* inputBufferPtr, int inputBufferSize, long* outputBuffer, int outputBufferSize)
        {
            Debug.Assert(inputBufferSize == state.BufferSize);

            var stateBitPos = state._bitPos;
          
            var headerBits = Read2(stateBitPos, inputBufferPtr, inputBufferSize, out stateBitPos);
            if (headerBits == 0b11) // Extended value, now figure out which one...
            {
                headerBits = Read2(stateBitPos, inputBufferPtr, inputBufferSize, out stateBitPos);
                switch (headerBits)
                {
                    // end of entries marker
                    case 0b00:
                        // We found an end of stream marker, but we may be called on this again, so ensure that we always
                        // return no results here by *not* updating the bit pos, so the next call would reach here too
                        //state._bitPos = stateBitPos;
                        return 0;
                    case 0b10: // raw value, return directly
                        var value = (long)Read(stateBitPos, inputBufferPtr, 63, inputBufferSize, out stateBitPos);
                        state._prevValue = value;
                        state._bitPos = stateBitPos;
                        state.NumberOfReads++;
                        outputBuffer[0] = value;
                        return 1;
                    default:
                        throw new ArgumentOutOfRangeException(headerBits + " isn't a valid extended header marker");
                }
            }

            if (headerBits >= 0b11)
                throw new ArgumentOutOfRangeException(headerBits + " isn't a valid header marker");

            var bitsToRead = headerBits == 0b00 ? 7 : 13;
            var bits = Read(stateBitPos, inputBufferPtr, bitsToRead, inputBufferSize, out stateBitPos);

            int numOfBits = (int)(0x1F & bits);
            if (numOfBits == 0)
                return 0;

            long statePrevValue = state._prevValue;
            int numOfRepeatedValues = headerBits == 0b00 ? NumberOfValues[(int)(bits >> 5)] : (int)(bits >> 5);
            
            if (numOfRepeatedValues > outputBufferSize)
                throw new ArgumentOutOfRangeException(nameof(outputBufferSize), "Invalid size for PForDecoder, not enough space for values");
            
            if (headerBits == 0b10)
            {
                var repeatedDelta = (int)Read(stateBitPos, inputBufferPtr, numOfBits, inputBufferSize, out stateBitPos);
                for (int i = 0; i < numOfRepeatedValues; i++)
                {
                    statePrevValue += repeatedDelta;
                    outputBuffer[i] = statePrevValue;
                }
            }
            else
            {
                // We are sure they are 0b00 or 0b01            
                for (int i = 0; i < numOfRepeatedValues; i++)
                {
                    var v = Read(stateBitPos, inputBufferPtr, numOfBits, inputBufferSize, out stateBitPos);
                    statePrevValue += (int)v;
                    outputBuffer[i] = statePrevValue;
                }
            }

            state._bitPos = stateBitPos;
            state._prevValue = statePrevValue;
            state.NumberOfReads += numOfRepeatedValues;
            return numOfRepeatedValues;
        }

        private static ReadOnlySpan<byte> ShiftValue => new byte[] {
            7, 6, 5, 4, 3, 2, 1, 0,
            7, 6, 5, 4, 3, 2, 1, 0,
            7, 6, 5, 4, 3, 2, 1, 0,
            7, 6, 5, 4, 3, 2, 1, 0,
            7, 6, 5, 4, 3, 2, 1, 0,
            7, 6, 5, 4, 3, 2, 1, 0,
            7, 6, 5, 4, 3, 2, 1, 0,
            7, 6, 5, 4, 3, 2, 1, 0,
            7, 6, 5, 4, 3, 2, 1, 0 };

        private static ReadOnlySpan<byte> IndexOffset => new byte[] { 0, 1, 2, 3 };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static byte CreateShuffleControl(int z, int y, int x, int w)
        {
            return (byte)(z << 6 | y << 4 | x << 2 | w);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int HorizontalOr(in Vector128<int> x)
        {
            var shuf = Sse3.MoveHighAndDuplicate(x.AsSingle()).AsInt32();
            var sums = Or(x, shuf);
            shuf = Sse.MoveHighToLow(shuf.AsSingle(), sums.AsSingle()).AsInt32();
            sums = Or(sums, shuf);

            return ConvertToInt32(sums);
        }        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe static ulong Read(int stateBitPos, byte* inputBufferPtr, int bitsToRead, int inputBufferSize, out int outputStateBit)
        {
            var maxByteOffset = (stateBitPos + bitsToRead) / 8;
#if DEBUG
            if (maxByteOffset >= inputBufferSize)
                throw new ArgumentOutOfRangeException();
#endif
            //TODO: Ensure that we aren't reading beyond the end of the buffer!
            if (Avx2.IsSupported && maxByteOffset + 32 < inputBufferSize)
            {
                return ReadAvx2(stateBitPos, inputBufferPtr, bitsToRead, out outputStateBit);
            }
            return ReadScalar(stateBitPos, inputBufferPtr, bitsToRead, out outputStateBit);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private unsafe static ulong ReadAvx2(int stateBitPos, byte* inputBufferPtr, int bitsToRead, out int outputStateBit)
        {
            int shiftIndex = stateBitPos & 0x7;

            ulong value = 0;
            outputStateBit = stateBitPos + bitsToRead;

            int* shiftTable = IntShiftTable + shiftIndex;

            // This is the larger valid index.
            var maxStateBitPos = Vector128.Create(outputStateBit - 1);

            // This has the form of v = { pos+0, pos+1, pos+2, pos+3 }            
            var stateBitVector = Add(Vector128.Create(stateBitPos), Vector128.Create(0, 1, 2, 3));
            while (bitsToRead >= 4)
            {
                bitsToRead -= 4;

                // We load from memory at different offsets. 
                var inputBufferValues = GatherVector128((int*)inputBufferPtr, ShiftRightLogical(stateBitVector, 3), 1);

                // We load from the table the variable shifts and
                var shiftTableValues = LoadVector128(shiftTable);
                var resultValues = And(ShiftRightLogicalVariable(inputBufferValues, shiftTableValues.AsUInt32()), Vector128.Create(1));
                resultValues = ShiftLeftLogicalVariable(resultValues, Vector128.Create(3u, 2u, 1u, 0u));

                value <<= 4;
                value |= (uint)HorizontalOr(resultValues);

                stateBitVector = Add(stateBitVector, Vector128.Create(4));

                shiftTable += 4;
                stateBitPos += 4;
            }

            if (bitsToRead == 0)
                goto End;

            shiftIndex += 3;

            value <<= 1;
            value |= (uint)((inputBufferPtr[(stateBitPos) >> 3] >> shiftTable[0]) & 1);
            if (bitsToRead == 1)
                goto End;

            value <<= 1;
            value |= (uint)((inputBufferPtr[(stateBitPos + 1) >> 3] >> shiftTable[1]) & 1);
            if (bitsToRead == 2)
                goto End;

            value <<= 1;
            value |= (uint)((inputBufferPtr[(stateBitPos + 2) >> 3] >> shiftTable[2]) & 1);

        End:
            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private unsafe static ulong ReadScalar(int stateBitPos, byte* inputBufferPtr, int bitsToRead, out int outputStateBit)
        {
            int shiftIndex = stateBitPos & 0x7;

            ulong value = 0;
            outputStateBit = stateBitPos + bitsToRead;

            fixed (byte* shiftTablePtr = ShiftValue)
            {
                byte* shiftTable = shiftTablePtr + shiftIndex;

                while (bitsToRead >= 4)
                {
                    bitsToRead -= 4;
                    stateBitPos += 4;

                    var v4 = (ulong)((inputBufferPtr[(stateBitPos - 1) >> 3] >> shiftTable[3]) & 1);
                    var v3 = (ulong)((inputBufferPtr[(stateBitPos - 2) >> 3] >> shiftTable[2]) & 1) << 1;
                    var v2 = (ulong)((inputBufferPtr[(stateBitPos - 3) >> 3] >> shiftTable[1]) & 1) << 2;
                    var v1 = (ulong)((inputBufferPtr[(stateBitPos - 4) >> 3] >> shiftTable[0]) & 1) << 3;

                    value = (value << 4) | v1 | v2 | v3 | v4;

                    shiftTable += 4;
                }

                if (bitsToRead == 0)
                    goto End;

                shiftIndex += 3;

                value <<= 1;
                value |= (uint)((inputBufferPtr[(stateBitPos) >> 3] >> shiftTable[0]) & 1);
                if (bitsToRead == 1)
                    goto End;

                value <<= 1;
                value |= (uint)((inputBufferPtr[(stateBitPos + 1) >> 3] >> shiftTable[1]) & 1);
                if (bitsToRead == 2)
                    goto End;

                value <<= 1;
                value |= (uint)((inputBufferPtr[(stateBitPos + 2) >> 3] >> shiftTable[2]) & 1);

                End:
                return value;
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong Read2(in int stateBitPos, byte* inputBufferPtr, int inputBufferSize, out int outputStateBit)
        {
            int shiftIndex = stateBitPos & 0x7;
#if DEBUG
            if ((shiftIndex + 2)/8 >= inputBufferSize)
                throw new ArgumentOutOfRangeException();
#endif

            fixed (byte* shiftTablePtr = ShiftValue)
            {
                byte* shiftTable = shiftTablePtr + shiftIndex;

                ulong value = (ulong)(uint)((inputBufferPtr[(stateBitPos + 0) >> 3] >> shiftTable[0]) & 1) << 1 |
                              (ulong)(uint)((inputBufferPtr[(stateBitPos + 1) >> 3] >> shiftTable[1]) & 1);

                outputStateBit = stateBitPos + 2;
                return value;
            }
        }

        public static List<long> GetDebugOutput(Span<byte> buf)
        {
            Span<long> scratch = stackalloc long[128];
            
            var list = new List<long>();
            var state = new DecoderState(buf.Length);
            while (true)
            {
                var len = Decode(ref state, buf, scratch);
                if (len == 0)
                    break;

                for (int i = 0; i < len; i++)
                {
                    list.Add(scratch[i]);
                }
            }
            return list;
        }

        public static int ReadCount(Span<byte> output)
        {
            fixed (byte* p = output)
            {
                BlittableJsonReaderBase.ReadVariableSizeLongInReverse(p, output.Length - 1, out byte pos);
                int count = BlittableJsonReaderBase.ReadVariableSizeIntInReverse(p, output.Length - 1 - pos, out _);
                return count;
            }
        }
        
        public static long ReadLast(Span<byte> output)
        {
            fixed (byte* p = output)
            {
                long len = BlittableJsonReaderBase.ReadVariableSizeLongInReverse(p, output.Length - 1, out _);
                return len;
            }
        }
    }
}
