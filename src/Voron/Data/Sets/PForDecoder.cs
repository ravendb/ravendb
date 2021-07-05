using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

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

        public static int Decode(ref DecoderState state, in Span<byte> inputBuffer, in Span<int> outputBuffer)
        {
            Debug.Assert(inputBuffer.Length == state.BufferSize);

            var bits = Read(ref state, inputBuffer, 2);
            switch (bits)
            {
                case 0b00: // fixed header
                    bits = Read(ref state, inputBuffer, 7);
                    int numOfValues = (bits >> 5) switch
                    {
                        0b000 => 1,
                        0b001 => 32,
                        0b010 => 64,
                        0b011 => 128,
                        _ => throw new ArgumentOutOfRangeException((bits >> 5) + " isn't a valid number of items for fixed header")
                    };
                    return ReadNumbers(ref state, inputBuffer, outputBuffer, (int)(0x1F & bits), numOfValues);
                case 0b01: // variable size
                    bits = Read(ref state, inputBuffer, 13);
                    return ReadNumbers(ref state, inputBuffer, outputBuffer, (int)(0x1F & bits), (int)(bits >> 5));
                case 0b10: // repeated header
                    bits = Read(ref state, inputBuffer, 13);
                    int numOfRepeatedValues = (int)(bits >> 5);
                    int numOfBits = (int)(0x1F & bits);
                    var repeatedDelta = (int)Read(ref state, inputBuffer, numOfBits);
                    for (int i = 0; i < numOfRepeatedValues; i++)
                    {
                        state._prevValue += repeatedDelta;
                        outputBuffer[i] = state._prevValue;
                    }

                    state.NumberOfReads += numOfRepeatedValues;
                    return numOfRepeatedValues;
                case 0b11:
                    return 0;
                default:
                    throw new ArgumentOutOfRangeException(bits + " isn't a valid header marker");
            }
        }

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

        private static ulong Read(ref DecoderState state, in Span<byte> inputBuffer, int bitsToRead)
        {
            int end = state._bitPos + bitsToRead;
            if (end > state.MaxBits)
                throw new EndOfStreamException();

            ulong value = 0;
            while (state._bitPos < end)
            {
                value <<= 1;
                ulong bit = (ulong)(inputBuffer[state._bitPos >> 3] >> 7 - (state._bitPos & 0x7) & 1);
                value += bit;
                state._bitPos++;
            }
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
