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
        public static partial class Streamed
        {
            public class XXHash32Context
            {
                internal uint Seed = 0;
                internal XXHash32Values Current;
                internal readonly byte[] Leftover = new byte[XXHash32.Alignment];
                internal int LeftoverCount = 0;
                internal int BufferSize = 0;
            }

            public static unsafe class XXHash32
            {
                public const int Alignment = 16;


                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static XXHash32Context BeginProcessInline(uint seed = 0)
                {
                    var context = new XXHash32Context
                    {
                        Seed = seed
                    };
                    
                    context.Current.V1 = seed + XXHash32Constants.PRIME32_1 + XXHash32Constants.PRIME32_2;
                    context.Current.V2 = seed + XXHash32Constants.PRIME32_2;
                    context.Current.V3 = seed + 0;
                    context.Current.V4 = seed - XXHash32Constants.PRIME32_1;

                    return context;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static XXHash32Context ProcessInline(XXHash32Context context, byte* buffer, int size)
                {
                    if (context.LeftoverCount != 0)
                        throw new NotSupportedException("Streaming process does not support resuming with buffers whose size is not 16 bytes aligned. Supporting it would impact performance.");

                    byte* bEnd = buffer + size;
                    byte* limit = bEnd - Alignment;

                    uint v1 = context.Current.V1;
                    uint v2 = context.Current.V2;
                    uint v3 = context.Current.V3;
                    uint v4 = context.Current.V4;

                    context.LeftoverCount = (int)(bEnd - buffer) % Alignment;

                    if (context.BufferSize + size >= Alignment)
                    {
                        while (buffer <= limit)
                        {
                            v1 += *((uint*)buffer) * XXHash32Constants.PRIME32_2;
                            buffer += sizeof(uint);
                            v2 += *((uint*)buffer) * XXHash32Constants.PRIME32_2;
                            buffer += sizeof(uint);
                            v3 += *((uint*)buffer) * XXHash32Constants.PRIME32_2;
                            buffer += sizeof(uint);
                            v4 += *((uint*)buffer) * XXHash32Constants.PRIME32_2;
                            buffer += sizeof(uint);

                            v1 = XXHashHelpers.RotateLeft32(v1, 13);
                            v2 = XXHashHelpers.RotateLeft32(v2, 13);
                            v3 = XXHashHelpers.RotateLeft32(v3, 13);
                            v4 = XXHashHelpers.RotateLeft32(v4, 13);

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

                    return context;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static uint EndProcessInline(XXHash32Context context)
                {
                    uint h32;

                    if (context.BufferSize >= Alignment)
                    {
                        uint v1 = context.Current.V1;
                        uint v2 = context.Current.V2;
                        uint v3 = context.Current.V3;
                        uint v4 = context.Current.V4;

                        h32 = XXHashHelpers.RotateLeft32(v1, 1) + XXHashHelpers.RotateLeft32(v2, 7) + XXHashHelpers.RotateLeft32(v3, 12) + XXHashHelpers.RotateLeft32(v4, 18);
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
                                h32 = XXHashHelpers.RotateLeft32(h32, 17) * XXHash32Constants.PRIME32_4;
                                buffer += 4;
                            }

                            while (buffer < bEnd)
                            {
                                h32 += (uint)(*buffer) * XXHash32Constants.PRIME32_5;
                                h32 = XXHashHelpers.RotateLeft32(h32, 11) * XXHash32Constants.PRIME32_1;
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

                public static XXHash32Context BeginProcess(uint seed = 0)
                {
                    return BeginProcessInline(seed);
                }

                public static uint EndProcess(XXHash32Context context)
                {
                    return EndProcessInline(context);
                }

                public static XXHash32Context Process(XXHash32Context context, byte* buffer, int size)
                {
                    return ProcessInline(context, buffer, size);
                }

                public static XXHash32Context Process(XXHash32Context context, byte[] value, int size = -1)
                {
                    if (size == -1)
                        size = value.Length;

                    fixed (byte* buffer = value)
                    {
                        return ProcessInline(context, buffer, size);
                    }
                }
            }



            public class XXHash64Context
            {
                internal ulong Seed = 0;
                internal XXHash64Values Current;
                internal readonly byte[] Leftover = new byte[XXHash64.Alignment];
                internal int LeftoverCount;
                internal int BufferSize = 0;
            }


            public static unsafe class XXHash64
            {
                public const int Alignment = 32;

                public static XXHash64Context BeginProcessInline(uint seed = 0)
                {
                    var context = new XXHash64Context
                    {
                        Seed = seed
                    };

                    context.Current.V1 = seed + XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_2;
                    context.Current.V2 = seed + XXHash64Constants.PRIME64_2;
                    context.Current.V3 = seed + 0;
                    context.Current.V4 = seed - XXHash64Constants.PRIME64_1;

                    return context;
                }

                public static XXHash64Context ProcessInline(XXHash64Context context, byte* buffer, int size)
                {
                    if (context.LeftoverCount != 0)
                        throw new NotSupportedException("Streaming process does not support resuming with buffers whose size is not 16 bytes aligned. Supporting it would impact performance.");

                    byte* bEnd = buffer + size;
                    byte* limit = bEnd - Alignment;

                    ulong v1 = context.Current.V1;
                    ulong v2 = context.Current.V2;
                    ulong v3 = context.Current.V3;
                    ulong v4 = context.Current.V4;

                    context.LeftoverCount = (int)(bEnd - buffer) % Alignment;

                    if (context.BufferSize + size >= Alignment)
                    {
                        while (buffer <= limit)
                        {
                            v1 += *((ulong*)buffer) * XXHash64Constants.PRIME64_2;
                            buffer += sizeof(ulong);
                            v2 += *((ulong*)buffer) * XXHash64Constants.PRIME64_2;
                            buffer += sizeof(ulong);
                            v3 += *((ulong*)buffer) * XXHash64Constants.PRIME64_2;
                            buffer += sizeof(ulong);
                            v4 += *((ulong*)buffer) * XXHash64Constants.PRIME64_2;
                            buffer += sizeof(ulong);

                            v1 = XXHashHelpers.RotateLeft64(v1, 31);
                            v2 = XXHashHelpers.RotateLeft64(v2, 31);
                            v3 = XXHashHelpers.RotateLeft64(v3, 31);
                            v4 = XXHashHelpers.RotateLeft64(v4, 31);

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

                    return context;
                }

                public static ulong EndProcessInline(XXHash64Context context)
                {
                    ulong h64;

                    if (context.BufferSize >= Alignment)
                    {
                        ulong v1 = context.Current.V1;
                        ulong v2 = context.Current.V2;
                        ulong v3 = context.Current.V3;
                        ulong v4 = context.Current.V4;

                        h64 = XXHashHelpers.RotateLeft64(v1, 1) + XXHashHelpers.RotateLeft64(v2, 7) + XXHashHelpers.RotateLeft64(v3, 12) + XXHashHelpers.RotateLeft64(v4, 18);

                        v1 *= XXHash64Constants.PRIME64_2;
                        v1 = XXHashHelpers.RotateLeft64(v1, 31);
                        v1 *= XXHash64Constants.PRIME64_1;
                        h64 ^= v1;
                        h64 = h64 * XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_4;

                        v2 *= XXHash64Constants.PRIME64_2;
                        v2 = XXHashHelpers.RotateLeft64(v2, 31);
                        v2 *= XXHash64Constants.PRIME64_1;
                        h64 ^= v2;
                        h64 = h64 * XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_4;

                        v3 *= XXHash64Constants.PRIME64_2;
                        v3 = XXHashHelpers.RotateLeft64(v3, 31);
                        v3 *= XXHash64Constants.PRIME64_1;
                        h64 ^= v3;
                        h64 = h64 * XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_4;

                        v4 *= XXHash64Constants.PRIME64_2;
                        v4 = XXHashHelpers.RotateLeft64(v4, 31);
                        v4 *= XXHash64Constants.PRIME64_1;
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
                                k1 = XXHashHelpers.RotateLeft64(k1, 31);
                                k1 *= XXHash64Constants.PRIME64_1;
                                h64 ^= k1;
                                h64 = XXHashHelpers.RotateLeft64(h64, 27) * XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_4;
                                buffer += 8;
                            }

                            if (buffer + 4 <= bEnd)
                            {
                                h64 ^= *(uint*)buffer * XXHash64Constants.PRIME64_1;
                                h64 = XXHashHelpers.RotateLeft64(h64, 23) * XXHash64Constants.PRIME64_2 + XXHash64Constants.PRIME64_3;
                                buffer += 4;
                            }

                            while (buffer < bEnd)
                            {
                                h64 ^= ((ulong)*buffer) * XXHash64Constants.PRIME64_5;
                                h64 = XXHashHelpers.RotateLeft64(h64, 11) * XXHash64Constants.PRIME64_1;
                                buffer++;
                            }
                        }
                    }

                    h64 ^= h64 >> 33;
                    h64 *= XXHash64Constants.PRIME64_2;
                    h64 ^= h64 >> 29;
                    h64 *= XXHash64Constants.PRIME64_3;
                    h64 ^= h64 >> 32; ;

                    return h64;
                }


                public static XXHash64Context BeginProcess(uint seed = 0)
                {
                    return BeginProcessInline(seed);
                }

                public static ulong EndProcess(XXHash64Context context)
                {
                    return EndProcessInline(context);
                }

                public static XXHash64Context Process(XXHash64Context context, byte* buffer, int size)
                {
                    return ProcessInline(context, buffer, size);
                }

                public static XXHash64Context Process(XXHash64Context context, byte[] value, int size = -1)
                {
                    if (size == -1)
                        size = value.Length;

                    fixed (byte* buffer = value)
                    {
                        return ProcessInline(context, buffer, size);
                    }
                }
            }
        }
    }
}
