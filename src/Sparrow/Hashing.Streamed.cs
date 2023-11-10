using Sparrow.Binary;
using System;
using System.Runtime.CompilerServices;

namespace Sparrow
{
    internal partial class Hashing
    {
        internal static class Streamed
        {
            

            internal unsafe struct XXHash64Context
            {
                public ulong Seed;
                internal XXHash64Values Current;
                internal fixed byte Leftover[XXHash64.Alignment];
                internal int LeftoverCount;
                internal int BufferSize;
            }


            internal static unsafe class XXHash64
            {
                public const int Alignment = 32;

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static void Begin(ref XXHash64Context context)
                {
                    var seed = context.Seed;
                    context.Current.V1 = seed + XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_2;
                    context.Current.V2 = seed + XXHash64Constants.PRIME64_2;
                    context.Current.V3 = seed + 0;
                    context.Current.V4 = seed - XXHash64Constants.PRIME64_1;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static void Process(ref XXHash64Context context, byte* buffer, int size)
                {
                    if (context.LeftoverCount != 0)
                        throw new NotSupportedException("Streaming process does not support resuming with buffers whose size is not 16 bytes aligned. Supporting it would impact performance.");

                    byte* bEnd = buffer + size;
                    byte* limit = bEnd - Alignment;

                    context.LeftoverCount = (int)(bEnd - buffer) % Alignment;

                    if (context.BufferSize + size >= Alignment)
                    {
                        ulong v1 = context.Current.V1;
                        ulong v2 = context.Current.V2;
                        ulong v3 = context.Current.V3;
                        ulong v4 = context.Current.V4;

                        while (buffer <= limit)
                        {
                            v1 = Bits.RotateLeft64(v1 + ((ulong*)buffer)[0] * XXHash64Constants.PRIME64_2, 31) * XXHash64Constants.PRIME64_1;
                            v2 = Bits.RotateLeft64(v2 + ((ulong*)buffer)[1] * XXHash64Constants.PRIME64_2, 31) * XXHash64Constants.PRIME64_1;
                            v3 = Bits.RotateLeft64(v3 + ((ulong*)buffer)[2] * XXHash64Constants.PRIME64_2, 31) * XXHash64Constants.PRIME64_1;
                            v4 = Bits.RotateLeft64(v4 + ((ulong*)buffer)[3] * XXHash64Constants.PRIME64_2, 31) * XXHash64Constants.PRIME64_1;

                            buffer += 4 * sizeof(ulong);
                            context.BufferSize += Alignment;
                        }

                        context.Current.V1 = v1;
                        context.Current.V2 = v2;
                        context.Current.V3 = v3;
                        context.Current.V4 = v4;
                    }

                    for (int i = 0; i < context.LeftoverCount; i++)
                    {
                        context.Leftover[i] = *buffer;
                        buffer++;
                    }
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static ulong End(ref XXHash64Context context)
                {
                    ulong h64;

                    if (context.BufferSize >= Alignment)
                    {
                        ulong v1 = context.Current.V1;
                        ulong v2 = context.Current.V2;
                        ulong v3 = context.Current.V3;
                        ulong v4 = context.Current.V4;

                        h64 = Bits.RotateLeft64(v1, 1) + Bits.RotateLeft64(v2, 7) + Bits.RotateLeft64(v3, 12) + Bits.RotateLeft64(v4, 18);

                        v1 = Bits.RotateLeft64(v1 * XXHash64Constants.PRIME64_2, 31) * XXHash64Constants.PRIME64_1;
                        h64 = (h64 ^ v1) * XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_4;

                        v2 = Bits.RotateLeft64(v2 * XXHash64Constants.PRIME64_2, 31) * XXHash64Constants.PRIME64_1;
                        h64 = (h64 ^ v2) * XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_4;

                        v3 = Bits.RotateLeft64(v3 * XXHash64Constants.PRIME64_2, 31) * XXHash64Constants.PRIME64_1;
                        h64 = (h64 ^ v3) * XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_4;

                        v4 = Bits.RotateLeft64(v4 * XXHash64Constants.PRIME64_2, 31) * XXHash64Constants.PRIME64_1;
                        h64 = (h64 ^ v4) * XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_4;
                    }
                    else
                    {
                        h64 = context.Seed + XXHash64Constants.PRIME64_5;
                    }

                    h64 += (uint)(context.BufferSize + context.LeftoverCount);

                    if (context.LeftoverCount > 0)
                    {
                        fixed (byte* b = context.Leftover)
                        {
                            byte* buffer = b;
                            byte* bEnd = b + context.LeftoverCount;

                            while (buffer + 8 <= bEnd)
                            {
                                ulong k1 = Bits.RotateLeft64(*(ulong*)buffer * XXHash64Constants.PRIME64_2, 31) * XXHash64Constants.PRIME64_1;
                                h64 = Bits.RotateLeft64(h64 ^ k1, 27) * XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_4;
                                buffer += 8;
                            }

                            if (buffer + 4 <= bEnd)
                            {
                                h64 = Bits.RotateLeft64(h64 ^(*(uint*)buffer * XXHash64Constants.PRIME64_1), 23) * XXHash64Constants.PRIME64_2 + XXHash64Constants.PRIME64_3;
                                buffer += 4;
                            }

                            while (buffer < bEnd)
                            {
                                h64 = Bits.RotateLeft64(h64 ^(*buffer * XXHash64Constants.PRIME64_5), 11) * XXHash64Constants.PRIME64_1;
                                buffer++;
                            }
                        }
                    }

                    h64 ^= h64 >> 33;
                    h64 *= XXHash64Constants.PRIME64_2;
                    h64 ^= h64 >> 29;
                    h64 *= XXHash64Constants.PRIME64_3;
                    h64 ^= h64 >> 32;

                    return h64;
                }

                public static void Process(ref XXHash64Context context, byte[] value, int size = -1)
                {
                    if (size == -1)
                        size = value.Length;

                    fixed (byte* buffer = value)
                    {
                        Process(ref context, buffer, size);
                    }
                }
            }
        }
    }
}
