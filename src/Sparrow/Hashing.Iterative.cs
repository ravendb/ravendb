using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow
{
    partial class Hashing
    {
        public static class Iterative
        {
            public class XXHash32Block
            {
                private static readonly XXHash32Values[] Empty = new XXHash32Values[0];

                public readonly uint Seed;
                public readonly XXHash32Values[] Values;                

                public XXHash32Block(XXHash32Values[] values, uint seed = 0)
                {
                    this.Seed = seed;
                    this.Values = values;
                }                

                public XXHash32Block(int iterations, uint seed = 0)
                {
                    this.Seed = seed;
                    this.Values = new XXHash32Values[iterations];
                }

                public XXHash32Block(uint seed = 0)
                {
                    this.Seed = seed;
                    this.Values = Empty;
                }
            }

            public static unsafe class XXHash32
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static unsafe uint CalculateInline(byte* buffer, int len, XXHash32Block context, int startFrom = int.MaxValue)
                {
                    Contract.Requires(buffer != null);
                    Contract.Requires(context != null);
                    Contract.Requires(len >= 0);
                    Contract.Requires(startFrom < len || startFrom == int.MaxValue);

                    unchecked
                    {
                        uint h32;                        

                        byte* bEnd = buffer + len;
                        if (len >= 16)
                        {
                            byte* limit = bEnd - 16;

                            int iterations = Math.Min((int)((bEnd - buffer) / (4 * sizeof(uint))), startFrom / (4 * sizeof(uint)));

                            int bucketNumber = Math.Min(iterations, context.Values.Length);

                            // Retrieve the preprocessed context
                            var state = context.Values[bucketNumber];

                            // Advance the buffer to the position
                            buffer += iterations * 4 * sizeof(uint);

                            while (buffer <= limit)
                            {
                                state.V1 += *((uint*)buffer) * XXHash32Constants.PRIME32_2;
                                buffer += sizeof(uint);
                                state.V2 += *((uint*)buffer) * XXHash32Constants.PRIME32_2;
                                buffer += sizeof(uint);
                                state.V3 += *((uint*)buffer) * XXHash32Constants.PRIME32_2;
                                buffer += sizeof(uint);
                                state.V4 += *((uint*)buffer) * XXHash32Constants.PRIME32_2;
                                buffer += sizeof(uint);

                                state.V1 = Bits.RotateLeft32(state.V1, 13);
                                state.V2 = Bits.RotateLeft32(state.V2, 13);
                                state.V3 = Bits.RotateLeft32(state.V3, 13);
                                state.V4 = Bits.RotateLeft32(state.V4, 13);

                                state.V1 *= XXHash32Constants.PRIME32_1;
                                state.V2 *= XXHash32Constants.PRIME32_1;
                                state.V3 *= XXHash32Constants.PRIME32_1;
                                state.V4 *= XXHash32Constants.PRIME32_1;
                            }

                            h32 = Bits.RotateLeft32(state.V1, 1) + Bits.RotateLeft32(state.V2, 7) + Bits.RotateLeft32(state.V3, 12) + Bits.RotateLeft32(state.V4, 18);
                        }
                        else
                        {
                            h32 = context.Seed + XXHash32Constants.PRIME32_5;
                        }

                        h32 += (uint)len;

                        while (buffer + 4 <= bEnd)
                        {
                            h32 += *((uint*)buffer) * XXHash32Constants.PRIME32_3;
                            h32 = Bits.RotateLeft32(h32, 17) * XXHash32Constants.PRIME32_4;
                            buffer += 4;
                        }

                        while (buffer < bEnd)
                        {
                            h32 += (uint)(*buffer) * XXHash32Constants.PRIME32_5;
                            h32 = Bits.RotateLeft32(h32, 11) * XXHash32Constants.PRIME32_1;
                            buffer++;
                        }

                        h32 ^= h32 >> 15;
                        h32 *= XXHash32Constants.PRIME32_2;
                        h32 ^= h32 >> 13;
                        h32 *= XXHash32Constants.PRIME32_3;
                        h32 ^= h32 >> 16;

                        return h32;
                    }                    
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static unsafe XXHash32Block PreprocessInline(byte* buffer, int len, uint seed = 0)
                {
                    Contract.Requires(buffer != null);
                    Contract.Requires(len >= 0);

                    if (len >= 16)
                    {
                        byte* bEnd = buffer + len;

                        uint v1 = seed + XXHash32Constants.PRIME32_1 + XXHash32Constants.PRIME32_2;
                        uint v2 = seed + XXHash32Constants.PRIME32_2;
                        uint v3 = seed + 0;
                        uint v4 = seed - XXHash32Constants.PRIME32_1;

                        int iterations = (int)((bEnd - buffer) / (4 * sizeof(uint))) + 1;  

                        var values = new XXHash32Values[iterations];
                        values[0].V1 = v1;
                        values[0].V2 = v2;
                        values[0].V3 = v3;
                        values[0].V4 = v4;

                        for ( int i = 1; i < iterations; i++ )
                        {
                            v1 += ((uint*)buffer)[0] * XXHash32Constants.PRIME32_2;
                            v2 += ((uint*)buffer)[1] * XXHash32Constants.PRIME32_2;
                            v3 += ((uint*)buffer)[2] * XXHash32Constants.PRIME32_2;
                            v4 += ((uint*)buffer)[3] * XXHash32Constants.PRIME32_2;

                            buffer += 4 * sizeof(uint);

                            v1 = Bits.RotateLeft32(v1, 13);
                            v2 = Bits.RotateLeft32(v2, 13);
                            v3 = Bits.RotateLeft32(v3, 13);
                            v4 = Bits.RotateLeft32(v4, 13);

                            v1 *= XXHash32Constants.PRIME32_1;
                            v2 *= XXHash32Constants.PRIME32_1;
                            v3 *= XXHash32Constants.PRIME32_1;
                            v4 *= XXHash32Constants.PRIME32_1;
                            
                            values[i].V1 = v1;
                            values[i].V2 = v2;
                            values[i].V3 = v3;
                            values[i].V4 = v4;
                        }
                        
                        return new XXHash32Block(values, seed);
                    }
                    else return new XXHash32Block (seed); // No preprocess happens.
                }

                public static unsafe uint Calculate(byte* buffer, int len, XXHash32Block context, int startFrom = int.MaxValue)
                {
                    return CalculateInline(buffer, len, context, startFrom);
                }

                public static uint Calculate(string value, Encoding encoder, XXHash32Block context, int startFrom = int.MaxValue)
                {
                    var buf = encoder.GetBytes(value);

                    fixed (byte* buffer = buf)
                    {
                        return CalculateInline(buffer, buf.Length, context, startFrom);
                    }
                }
                public static uint CalculateRaw(string buf, int len, XXHash32Block context, int startFrom = int.MaxValue)
                {
                    fixed (char* buffer = buf)
                    {
                        return CalculateInline((byte*)buffer, len * sizeof(char), context, startFrom);
                    }
                }

                public static uint Calculate(byte[] buf, int len, XXHash32Block context, int startFrom = int.MaxValue)
                {
                    if (len == -1)
                        len = buf.Length;

                    fixed (byte* buffer = buf)
                    {
                        return CalculateInline(buffer, len, context, startFrom);
                    }
                }

                public static uint Calculate(int[] buf, int len, XXHash32Block context, int startFrom = int.MaxValue)
                {
                    if (len == -1)
                        len = buf.Length;

                    fixed (int* buffer = buf)
                    {
                        return CalculateInline((byte*)buffer, len * sizeof(int), context, startFrom);
                    }
                }

                public static unsafe XXHash32Block Preprocess(byte* buffer, int len, uint seed = 0)
                {
                    return PreprocessInline(buffer, len, seed);
                }

                public static XXHash32Block Preprocess(string value, Encoding encoder, uint seed = 0)
                {
                    var buf = encoder.GetBytes(value);

                    fixed (byte* buffer = buf)
                    {
                        return PreprocessInline(buffer, buf.Length, seed);
                    }
                }

                public static XXHash32Block PreprocessRaw(string buf, uint seed = 0)
                {
                    fixed (char* buffer = buf)
                    {
                        return PreprocessInline((byte*)buffer, buf.Length * sizeof(char), seed);
                    }
                }

                public static XXHash32Block Preprocess(byte[] buf, int len = -1, uint seed = 0)
                {
                    if (len == -1)
                        len = buf.Length;

                    fixed (byte* buffer = buf)
                    {
                        return PreprocessInline(buffer, len, seed);
                    }
                }

                public static XXHash32Block Preprocess(int[] buf, int len = -1, uint seed = 0)
                {
                    if (len == -1)
                        len = buf.Length;

                    fixed (int* buffer = buf)
                    {
                        return PreprocessInline((byte*)buffer, len * sizeof(int), seed);
                    }
                }

                public static XXHash32Block Preprocess(ulong[] buf, int len = -1, uint seed = 0)
                {
                    if (len == -1)
                        len = buf.Length;

                    fixed (ulong* buffer = buf)
                    {
                        return PreprocessInline((byte*)buffer, len * sizeof(ulong), seed);
                    }
                }
            }
        }
    }
}
