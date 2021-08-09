using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Xml;

namespace Voron.Data.Sets
{

    public static class PForDecoder
    {
        public struct DecoderState
        {
            public readonly int BufferSize;
            public readonly int MaxBits => BufferSize * 8;

            public int NumberOfReads;

            internal int _bitPos;
            internal int _prevValue;    
            
            public DecoderState(int bufferSize)
            {
                _bitPos = 0;
                _prevValue = 0;
                NumberOfReads = 0;
                BufferSize = bufferSize;
            }
        }

        public static DecoderState Initialize(Span<byte> inputBuffer)
        {
            return new DecoderState(inputBuffer.Length);
        }

        private static ReadOnlySpan<int> NumberOfValues => new int[] { 1, 32, 64, 128 };
        
        public static int Decode(ref DecoderState state, in Span<byte> inputBuffer, in Span<int> outputBuffer)
        {
            Debug.Assert(inputBuffer.Length == state.BufferSize);

            var headerBits = Read(ref state, inputBuffer, 2);
            if (headerBits == 0b11)
                return 0;

            if (headerBits >= 0b11)
                throw new ArgumentOutOfRangeException(headerBits + " isn't a valid header marker");

            var bitsToRead = headerBits == 0b00 ? 7 : 13;
            var bits = Read(ref state, inputBuffer, bitsToRead);

            int numOfRepeatedValues = headerBits == 0b00 ? NumberOfValues[(int)(bits >> 5)] : (int)(bits >> 5);

            if (headerBits == 0b10)
            {
                int numOfBits = (int)(0x1F & bits);
                var repeatedDelta = (int)Read(ref state, inputBuffer, numOfBits);
                for (int i = 0; i < numOfRepeatedValues; i++)
                {
                    state._prevValue += repeatedDelta;
                    outputBuffer[i] = state._prevValue;
                }
                state.NumberOfReads += numOfRepeatedValues;
                return numOfRepeatedValues;
            }

            // We are sure they are 0b00 or 0b01
            return ReadNumbers(ref state, inputBuffer, outputBuffer, (int)(0x1F & bits), numOfRepeatedValues);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int ReadNumbers(ref DecoderState state, in Span<byte> inputBuffer, in Span<int> outputBuffer, int numOfBits, int numOfValues)
        {
            if (numOfBits == 0)
                return 0;

            for (int i = 0; i < numOfValues; i++)
            {
                var v = Read(ref state, inputBuffer, numOfBits);
                state._prevValue += (int)v;
                outputBuffer[i] = state._prevValue;
            }

            state.NumberOfReads += numOfValues;
            return numOfValues;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong Read(ref DecoderState state, in ReadOnlySpan<byte> inputBuffer, int bitsToRead)
{
            int stateBitPos = state._bitPos;
            int end = stateBitPos + bitsToRead;
            if (end > state.MaxBits)
                throw new EndOfStreamException();

            ulong value = 0;
            while (stateBitPos < end)
            {
                value <<= 1;
                value += (ulong)(inputBuffer[stateBitPos >> 3] >> 7 - (stateBitPos & 0x7) & 1);
                stateBitPos++;
            }

            state._bitPos = stateBitPos;
            return value;
        }

        public static List<int> GetDebugOutput(Span<byte> buf)
        {
            Span<int> scratch = stackalloc int[128];
            
            var list = new List<int>();
            var state = Initialize(buf);
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
    }
}
