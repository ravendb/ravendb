using Sparrow.Binary;
using System;
using System.Runtime.CompilerServices;

namespace Sparrow
{
    partial class Hashing
    {
        public static class Streamed
        {
            #region XXHash32 & XXHash64

            public unsafe struct XXHash32Context
            {
                internal uint Seed;
                internal XXHash32Values Current;
                internal fixed byte Leftover[XXHash32.Alignment];
                internal int LeftoverCount ;
                internal int BufferSize;
            }

            public static unsafe class XXHash32
            {
                public const int Alignment = 16;


                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static void BeginProcessInline(ref XXHash32Context context)
                {
                    var seed = context.Seed;
                    context.Current.V1 = seed + XXHash32Constants.PRIME32_1 + XXHash32Constants.PRIME32_2;
                    context.Current.V2 = seed + XXHash32Constants.PRIME32_2;
                    context.Current.V3 = seed + 0;
                    context.Current.V4 = seed - XXHash32Constants.PRIME32_1;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static void ProcessInline(ref XXHash32Context context, byte* buffer, int size)
                {
                    if (context.LeftoverCount != 0)
                        throw new NotSupportedException("Streaming process does not support resuming with buffers whose size is not 16 bytes aligned. Supporting it would impact performance.");

                    byte* bEnd = buffer + size;
                    byte* limit = bEnd - Alignment;

                    context.LeftoverCount = (int)(bEnd - buffer) % Alignment;

                    if (context.BufferSize + size >= Alignment)
                    {
                        uint v1 = context.Current.V1;
                        uint v2 = context.Current.V2;
                        uint v3 = context.Current.V3;
                        uint v4 = context.Current.V4;

                        while (buffer <= limit)
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

                            context.BufferSize += Alignment;
                        }

                        context.Current.V1 = v1;
                        context.Current.V2 = v2;
                        context.Current.V3 = v3;
                        context.Current.V4 = v4;
                    }

                    for(int i = 0; i < context.LeftoverCount; i++ )
                    {
                        context.Leftover[i] = *buffer;
                        buffer++;
                    }
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static uint EndProcessInline(ref XXHash32Context context)
                {
                    uint h32;

                    if (context.BufferSize >= Alignment)
                    {
                        uint v1 = context.Current.V1;
                        uint v2 = context.Current.V2;
                        uint v3 = context.Current.V3;
                        uint v4 = context.Current.V4;

                        h32 = Bits.RotateLeft32(v1, 1) + Bits.RotateLeft32(v2, 7) + Bits.RotateLeft32(v3, 12) + Bits.RotateLeft32(v4, 18);
                    }
                    else
                    {
                        h32 = context.Seed + XXHash32Constants.PRIME32_5;
                    }

                    h32 += (uint)(context.BufferSize + context.LeftoverCount);

                    if ( context.LeftoverCount > 0 )
                    {
                        fixed (byte* b = context.Leftover)
                        {
                            byte* buffer = b;
                            byte* bEnd = b + context.LeftoverCount;

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
                        }
                    }

                    h32 ^= h32 >> 15;
                    h32 *= XXHash32Constants.PRIME32_2;
                    h32 ^= h32 >> 13;
                    h32 *= XXHash32Constants.PRIME32_3;
                    h32 ^= h32 >> 16;

                    return h32;
                }

                public static void BeginProcess(ref XXHash32Context context)
                {
                    BeginProcessInline(ref context);
                }

                public static uint EndProcess(ref XXHash32Context context)
                {
                    return EndProcessInline(ref context);
                }

                public static void Process(ref XXHash32Context context, byte* buffer, int size)
                {
                    ProcessInline(ref context, buffer, size);
                }

                public static void Process(ref XXHash32Context context, byte[] value, int size = -1)
                {
                    if (size == -1)
                        size = value.Length;

                    fixed (byte* buffer = value)
                    {
                        ProcessInline(ref context, buffer, size);
                    }
                }
            }


            public unsafe struct XXHash64Context
            {
                public ulong Seed;
                internal XXHash64Values Current;
                internal fixed byte Leftover[XXHash64.Alignment];
                internal int LeftoverCount;
                internal int BufferSize;
            }


            public static unsafe class XXHash64
            {
                public const int Alignment = 32;

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static void BeginProcessInline(ref XXHash64Context context)
                {
                    var seed = context.Seed;
                    context.Current.V1 = seed + XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_2;
                    context.Current.V2 = seed + XXHash64Constants.PRIME64_2;
                    context.Current.V3 = seed + 0;
                    context.Current.V4 = seed - XXHash64Constants.PRIME64_1;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static void ProcessInline(ref XXHash64Context context, byte* buffer, int size)
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
                            v1 += ((ulong*)buffer)[0] * XXHash64Constants.PRIME64_2;
                            v2 += ((ulong*)buffer)[1] * XXHash64Constants.PRIME64_2;
                            v3 += ((ulong*)buffer)[2] * XXHash64Constants.PRIME64_2;
                            v4 += ((ulong*)buffer)[3] * XXHash64Constants.PRIME64_2;

                            buffer += 4 * sizeof(ulong);

                            v1 = Bits.RotateLeft64(v1, 31);
                            v2 = Bits.RotateLeft64(v2, 31);
                            v3 = Bits.RotateLeft64(v3, 31);
                            v4 = Bits.RotateLeft64(v4, 31);                            

                            v1 *= XXHash64Constants.PRIME64_1;
                            v2 *= XXHash64Constants.PRIME64_1;
                            v3 *= XXHash64Constants.PRIME64_1;
                            v4 *= XXHash64Constants.PRIME64_1;

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
                public static ulong EndProcessInline(ref XXHash64Context context)
                {
                    ulong h64;

                    if (context.BufferSize >= Alignment)
                    {
                        ulong v1 = context.Current.V1;
                        ulong v2 = context.Current.V2;
                        ulong v3 = context.Current.V3;
                        ulong v4 = context.Current.V4;

                        h64 = Bits.RotateLeft64(v1, 1) + Bits.RotateLeft64(v2, 7) + Bits.RotateLeft64(v3, 12) + Bits.RotateLeft64(v4, 18);

                        v1 *= XXHash64Constants.PRIME64_2;
                        v2 *= XXHash64Constants.PRIME64_2;
                        v3 *= XXHash64Constants.PRIME64_2;
                        v4 *= XXHash64Constants.PRIME64_2;

                        v1 = Bits.RotateLeft64(v1, 31);
                        v2 = Bits.RotateLeft64(v2, 31);
                        v3 = Bits.RotateLeft64(v3, 31);
                        v4 = Bits.RotateLeft64(v4, 31);

                        v1 *= XXHash64Constants.PRIME64_1;
                        v2 *= XXHash64Constants.PRIME64_1;
                        v3 *= XXHash64Constants.PRIME64_1;
                        v4 *= XXHash64Constants.PRIME64_1;

                        h64 ^= v1;
                        h64 = h64 * XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_4;

                        h64 ^= v2;
                        h64 = h64 * XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_4;

                        h64 ^= v3;
                        h64 = h64 * XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_4;

                        h64 ^= v4;
                        h64 = h64 * XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_4;
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
                                ulong k1 = *((ulong*)buffer);
                                k1 *= XXHash64Constants.PRIME64_2;
                                k1 = Bits.RotateLeft64(k1, 31);
                                k1 *= XXHash64Constants.PRIME64_1;
                                h64 ^= k1;
                                h64 = Bits.RotateLeft64(h64, 27) * XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_4;
                                buffer += 8;
                            }

                            if (buffer + 4 <= bEnd)
                            {
                                h64 ^= *(uint*)buffer * XXHash64Constants.PRIME64_1;
                                h64 = Bits.RotateLeft64(h64, 23) * XXHash64Constants.PRIME64_2 + XXHash64Constants.PRIME64_3;
                                buffer += 4;
                            }

                            while (buffer < bEnd)
                            {
                                h64 ^= ((ulong)*buffer) * XXHash64Constants.PRIME64_5;
                                h64 = Bits.RotateLeft64(h64, 11) * XXHash64Constants.PRIME64_1;
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


                public static void BeginProcess(ref XXHash64Context context)
                {
                    BeginProcessInline(ref context);
                }

                public static ulong EndProcess(ref XXHash64Context context)
                {
                    return EndProcessInline(ref context);
                }

                public static void Process(ref XXHash64Context context, byte* buffer, int size)
                {
                     ProcessInline(ref context, buffer, size);
                }

                public static void Process(ref XXHash64Context context, byte[] value, int size = -1)
                {
                    if (size == -1)
                        size = value.Length;

                    fixed (byte* buffer = value)
                    {
                        ProcessInline(ref context, buffer, size);
                    }
                }
            }
            #endregion

            #region Metro128


            public class Metro128Context
            {
                internal uint Seed = 0;
                internal Metro128Values Current;
                internal readonly byte[] Leftover = new byte[Metro128.Alignment];
                internal int LeftoverCount = 0;
                internal int BufferSize = 0;
            }



            public static unsafe class Metro128
            {
                public const int Alignment = 32;

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static Metro128Context BeginProcessInline(uint seed = 0)
                {
                    var context = new Metro128Context
                    {
                        Seed = seed
                    };

                    context.Current.V0 = (seed - Metro128Constants.K0) * Metro128Constants.K3;
                    context.Current.V1 = (seed + Metro128Constants.K1) * Metro128Constants.K2;
                    context.Current.V2 = (seed + Metro128Constants.K0) * Metro128Constants.K2;
                    context.Current.V3 = (seed - Metro128Constants.K1) * Metro128Constants.K3;  

                    return context;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static Metro128Context ProcessInline(Metro128Context context, byte* buffer, int length)
                {
                    if (context.LeftoverCount != 0)
                        throw new NotSupportedException("Streaming process does not support resuming with buffers whose size is not 32 bytes aligned. Supporting it would impact performance.");

                    byte* ptr = buffer;
                    byte* end = ptr + length;

                    context.LeftoverCount = (int)(end - ptr) % Alignment;

                    if (context.BufferSize + length >= Alignment)
                    {
                        ulong v0 = context.Current.V0;
                        ulong v1 = context.Current.V1;
                        ulong v2 = context.Current.V2;
                        ulong v3 = context.Current.V3;

                        while (ptr <= (end - 32))
                        {
                            v0 += ((ulong*)ptr)[0] * Metro128Constants.K0;
                            v1 += ((ulong*)ptr)[1] * Metro128Constants.K1;

                            v0 = Bits.RotateRight64(v0, 29) + v2;
                            v1 = Bits.RotateRight64(v1, 29) + v3;

                            v2 += ((ulong*)ptr)[2] * Metro128Constants.K2;
                            v3 += ((ulong*)ptr)[3] * Metro128Constants.K3;

                            v2 = Bits.RotateRight64(v2, 29) + v0;
                            v3 = Bits.RotateRight64(v3, 29) + v1;

                            ptr += Alignment;
                            context.BufferSize += Alignment;
                        }

                        context.Current.V0 = v0;
                        context.Current.V1 = v1;
                        context.Current.V2 = v2;
                        context.Current.V3 = v3;
                    }

                    for (int i = 0; i < context.LeftoverCount; i++)
                    {
                        context.Leftover[i] = *ptr;
                        ptr++;
                    }

                    return context;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static Metro128Hash EndProcessInline(Metro128Context context)
                {
                    ulong v0 = context.Current.V0;
                    ulong v1 = context.Current.V1;
                    ulong v2 = context.Current.V2;
                    ulong v3 = context.Current.V3;

                    if (context.BufferSize >= Alignment)
                    {
                        v2 ^= Bits.RotateRight64(((v0 + v3) * Metro128Constants.K0) + v1, 21) * Metro128Constants.K1;
                        v3 ^= Bits.RotateRight64(((v1 + v2) * Metro128Constants.K1) + v0, 21) * Metro128Constants.K0;
                        v0 ^= Bits.RotateRight64(((v0 + v2) * Metro128Constants.K0) + v3, 21) * Metro128Constants.K1;
                        v1 ^= Bits.RotateRight64(((v1 + v3) * Metro128Constants.K1) + v2, 21) * Metro128Constants.K0;
                    }

                    if (context.LeftoverCount > 0)
                    {
                        fixed (byte* b = context.Leftover)
                        {
                            byte* ptr = b;
                            byte* end = b + context.LeftoverCount;

                            if ((end - ptr) >= 16)
                            {
                                v0 += ((ulong*)ptr)[0] * Metro128Constants.K2;
                                v1 += ((ulong*)ptr)[1] * Metro128Constants.K2;

                                v0 = Bits.RotateRight64(v0, 33) * Metro128Constants.K3;
                                v1 = Bits.RotateRight64(v1, 33) * Metro128Constants.K3;

                                ptr += 2 * sizeof(ulong);

                                v0 ^= Bits.RotateRight64((v0 * Metro128Constants.K2) + v1, 45) * Metro128Constants.K1;
                                v1 ^= Bits.RotateRight64((v1 * Metro128Constants.K3) + v0, 45) * Metro128Constants.K0;
                            }

                            if ((end - ptr) >= 8)
                            {
                                v0 += *((ulong*)ptr) * Metro128Constants.K2; ptr += sizeof(ulong); v0 = Bits.RotateRight64(v0, 33) * Metro128Constants.K3;
                                v0 ^= Bits.RotateRight64((v0 * Metro128Constants.K2) + v1, 27) * Metro128Constants.K1;
                            }

                            if ((end - ptr) >= 4)
                            {
                                v1 += *((uint*)ptr) * Metro128Constants.K2; ptr += sizeof(uint); v1 = Bits.RotateRight64(v1, 33) * Metro128Constants.K3;
                                v1 ^= Bits.RotateRight64((v1 * Metro128Constants.K3) + v0, 46) * Metro128Constants.K0;
                            }

                            if ((end - ptr) >= 2)
                            {
                                v0 += *((ushort*)ptr) * Metro128Constants.K2; ptr += sizeof(ushort); v0 = Bits.RotateRight64(v0, 33) * Metro128Constants.K3;
                                v0 ^= Bits.RotateRight64((v0 * Metro128Constants.K2) + v1, 22) * Metro128Constants.K1;
                            }

                            if ((end - ptr) >= 1)
                            {
                                v1 += *((byte*)ptr) * Metro128Constants.K2; v1 = Bits.RotateRight64(v1, 33) * Metro128Constants.K3;
                                v1 ^= Bits.RotateRight64((v1 * Metro128Constants.K3) + v0, 58) * Metro128Constants.K0;
                            }
                        }
                    }

                    v0 += Bits.RotateRight64((v0 * Metro128Constants.K0) + v1, 13);
                    v1 += Bits.RotateRight64((v1 * Metro128Constants.K1) + v0, 37);
                    v0 += Bits.RotateRight64((v0 * Metro128Constants.K2) + v1, 13);
                    v1 += Bits.RotateRight64((v1 * Metro128Constants.K3) + v0, 37);

                    return new Metro128Hash { H1 = v0, H2 = v1 };
                }


                public static Metro128Context BeginProcess(uint seed = 0)
                {
                    return BeginProcessInline(seed);
                }

                public static Metro128Hash EndProcess(Metro128Context context)
                {
                    return EndProcessInline(context);
                }

                public static Metro128Context Process(Metro128Context context, byte* buffer, int size)
                {
                    return ProcessInline(context, buffer, size);
                }

                public static Metro128Context Process(Metro128Context context, byte[] value, int size = -1)
                {
                    if (size == -1)
                        size = value.Length;

                    fixed (byte* buffer = value)
                    {
                        return ProcessInline(context, buffer, size);
                    }
                }

            }

            #endregion
        }
    }
}
