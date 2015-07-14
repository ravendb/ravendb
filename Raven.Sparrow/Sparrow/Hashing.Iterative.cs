using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow
{
    partial class Hashing
    {
        public static partial class Iterative
        {
            public struct XXHash32Values
            {
                public uint V1;
                public uint V2;
                public uint V3;
                public uint V4;
            }

            public class XXHash32Block
            {
                private readonly static XXHash32Values[] Empty = new XXHash32Values[0];

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

            public unsafe static class XXHash32
            {
                public static unsafe uint CalculateInline(byte* buffer, int len, XXHash32Block context)
                {
                    unchecked
                    {
                        uint h32;

                        byte* bEnd = buffer + len;
                        if (len >= 16)
                        {
                            byte* limit = bEnd - 16;
                            int iterations = (int)((bEnd - buffer) / (4 * sizeof(uint)));

                            int bucketNumber = Math.Min(iterations, context.Values.Length) - 1;

                            // Retrieve the preprocessed context
                            var state = context.Values[bucketNumber];

                            // Advance the buffer to the position
                            buffer += iterations * 4 * sizeof(uint);

                            while (buffer <= limit)
                            {
                                state.V1 += *((uint*)buffer) * PRIME32_2;
                                buffer += sizeof(uint);
                                state.V2 += *((uint*)buffer) * PRIME32_2;
                                buffer += sizeof(uint);
                                state.V3 += *((uint*)buffer) * PRIME32_2;
                                buffer += sizeof(uint);
                                state.V4 += *((uint*)buffer) * PRIME32_2;
                                buffer += sizeof(uint);

                                state.V1 = RotateLeft32(state.V1, 13);
                                state.V2 = RotateLeft32(state.V2, 13);
                                state.V3 = RotateLeft32(state.V3, 13);
                                state.V4 = RotateLeft32(state.V4, 13);

                                state.V1 *= PRIME32_1;
                                state.V2 *= PRIME32_1;
                                state.V3 *= PRIME32_1;
                                state.V4 *= PRIME32_1;
                            }

                            h32 = RotateLeft32(state.V1, 1) + RotateLeft32(state.V2, 7) + RotateLeft32(state.V3, 12) + RotateLeft32(state.V4, 18);
                        }
                        else
                        {
                            h32 = context.Seed + PRIME32_5;
                        }

                        h32 += (uint)len;

                        while (buffer + 4 <= bEnd)
                        {
                            h32 += *((uint*)buffer) * PRIME32_3;
                            h32 = RotateLeft32(h32, 17) * PRIME32_4;
                            buffer += 4;
                        }

                        while (buffer < bEnd)
                        {
                            h32 += (uint)(*buffer) * PRIME32_5;
                            h32 = RotateLeft32(h32, 11) * PRIME32_1;
                            buffer++;
                        }

                        h32 ^= h32 >> 15;
                        h32 *= PRIME32_2;
                        h32 ^= h32 >> 13;
                        h32 *= PRIME32_3;
                        h32 ^= h32 >> 16;

                        return h32;
                    }                    
                }

                public static unsafe XXHash32Block PreprocessInline(byte* buffer, int len, uint seed = 0)
                {
                    if (len >= 16)
                    {
                        byte* bEnd = buffer + len;

                        uint v1 = seed + PRIME32_1 + PRIME32_2;
                        uint v2 = seed + PRIME32_2;
                        uint v3 = seed + 0;
                        uint v4 = seed - PRIME32_1;
                        
                        int iterations = (int)((bEnd - buffer) / (4 * sizeof(uint)));

                        var values = new XXHash32Values[iterations];
                        for ( int i = 0; i < iterations; i++ )
                        {
                            v1 += *((uint*)buffer) * PRIME32_2;
                            buffer += sizeof(uint);
                            v2 += *((uint*)buffer) * PRIME32_2;
                            buffer += sizeof(uint);
                            v3 += *((uint*)buffer) * PRIME32_2;
                            buffer += sizeof(uint);
                            v4 += *((uint*)buffer) * PRIME32_2;
                            buffer += sizeof(uint);

                            v1 = RotateLeft32(v1, 13);
                            v2 = RotateLeft32(v2, 13);
                            v3 = RotateLeft32(v3, 13);
                            v4 = RotateLeft32(v4, 13);

                            v1 *= PRIME32_1;
                            v2 *= PRIME32_1;
                            v3 *= PRIME32_1;
                            v4 *= PRIME32_1;
                            
                            values[i].V1 = v1;
                            values[i].V2 = v2;
                            values[i].V3 = v3;
                            values[i].V4 = v4;
                        }

                        return new XXHash32Block(values, seed);
                    }
                    else return new XXHash32Block (seed); // No preprocess happens.
                }

                public static unsafe uint Calculate(byte* buffer, int len, XXHash32Block context)
                {
                    return CalculateInline(buffer, len, context);
                }

                public static uint Calculate(string value, Encoding encoder, XXHash32Block context)
                {
                    var buf = encoder.GetBytes(value);

                    fixed (byte* buffer = buf)
                    {
                        return CalculateInline(buffer, buf.Length, context);
                    }
                }
                public static uint CalculateRaw(string buf, int len, XXHash32Block context)
                {
                    fixed (char* buffer = buf)
                    {
                        return CalculateInline((byte*)buffer, len * sizeof(char), context);
                    }
                }

                public static uint Calculate(byte[] buf, int len, XXHash32Block context)
                {
                    if (len == -1)
                        len = buf.Length;

                    fixed (byte* buffer = buf)
                    {
                        return CalculateInline(buffer, len, context);
                    }
                }

                public static uint Calculate(int[] buf, int len, XXHash32Block context)
                {
                    if (len == -1)
                        len = buf.Length;

                    fixed (int* buffer = buf)
                    {
                        return CalculateInline((byte*)buffer, len * sizeof(int), context);
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

                private static uint PRIME32_1 = 2654435761U;
                private static uint PRIME32_2 = 2246822519U;
                private static uint PRIME32_3 = 3266489917U;
                private static uint PRIME32_4 = 668265263U;
                private static uint PRIME32_5 = 374761393U;

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                private static uint RotateLeft32(uint value, int count)
                {
                    return (value << count) | (value >> (32 - count));
                }
            }
        }
    }
}
