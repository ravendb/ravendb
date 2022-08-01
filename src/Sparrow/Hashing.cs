using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Sparrow
{
    public struct Metro128Hash
    {
        public ulong H1;
        public ulong H2;

        public Metro128Hash(ulong h1, ulong h2)
        {
            H1 = h1;
            H2 = h2;
        }

        public byte[] ToByteArray()
        {
            var result = new byte[sizeof(ulong) * 2];
            unsafe
            {
                fixed( byte* ptr = result )
                {
                    ((ulong*)ptr)[0] = H1;
                    ((ulong*)ptr)[1] = H2;
                }
            }
            return result;
        }

        public static Metro128Hash FromByteArray(byte[] source)
        {
            if (source.Length != sizeof(ulong) * 2)
                throw new ArgumentException("Byte array is not a Metro128 hash.");

            unsafe
            {
                fixed (byte* ptr = source)
                {
                    return new Metro128Hash(((ulong*)ptr)[0], ((ulong*)ptr)[1]);
                }
            }
        }
    }

    public static unsafe partial class Hashing
    {
        #region XXHash32 & XXHash64

        public struct XXHash32Values
        {
            public uint V1;
            public uint V2;
            public uint V3;
            public uint V4;
        }

        internal static class XXHash32Constants
        {
            internal const uint PRIME32_1 = 2654435761U;
            internal const uint PRIME32_2 = 2246822519U;
            internal const uint PRIME32_3 = 3266489917U;
            internal const uint PRIME32_4 = 668265263U;
            internal const uint PRIME32_5 = 374761393U;
        }

        public interface ICharacterModifier
        {
            char Modify(char ch);
        }

        public struct OrdinalModifier : ICharacterModifier
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public char Modify(char ch)
            {
                return ch;
            }
        }

        public struct OrdinalIgnoreCaseModifier : ICharacterModifier
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public char Modify(char ch)
            {
                if (ch >= 65 && ch <= 90)
                    ch = (char)(ch | 0x0020);
                return ch;
            }
        }

        /// <summary>
        /// A port of the original XXHash algorithm from Google in 32bits 
        /// </summary>
        /// <remarks>The 32bits and 64bits hashes for the same data are different. In short those are 2 entirely different algorithms</remarks>
        public static class XXHash32
        {

            // TODO: Check if it is better to have ReadOnlySpan built on top of pointer or the other way around. 
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static uint CalculateInline(ReadOnlySpan<byte> buffer, uint seed = 0)
            {
                uint h32;

                ReadOnlySpan<uint> uintBuffer = MemoryMarshal.Cast<byte, uint>(buffer);

                int idx = 0;
                if (uintBuffer.Length >= 4)
                {
                    int limit = uintBuffer.Length - 4;

                    uint v1 = seed + XXHash32Constants.PRIME32_1 + XXHash32Constants.PRIME32_2;
                    uint v2 = seed + XXHash32Constants.PRIME32_2;
                    uint v3 = seed + 0;
                    uint v4 = seed - XXHash32Constants.PRIME32_1;

                    do
                    {
                        v1 += uintBuffer[idx + 0] * XXHash32Constants.PRIME32_2;
                        v2 += uintBuffer[idx + 1] * XXHash32Constants.PRIME32_2;
                        v3 += uintBuffer[idx + 2] * XXHash32Constants.PRIME32_2;
                        v4 += uintBuffer[idx + 3] * XXHash32Constants.PRIME32_2;

                        idx += 4;

                        v1 = Bits.RotateLeft32(v1, 13);
                        v2 = Bits.RotateLeft32(v2, 13);
                        v3 = Bits.RotateLeft32(v3, 13);
                        v4 = Bits.RotateLeft32(v4, 13);

                        v1 *= XXHash32Constants.PRIME32_1;
                        v2 *= XXHash32Constants.PRIME32_1;
                        v3 *= XXHash32Constants.PRIME32_1;
                        v4 *= XXHash32Constants.PRIME32_1;
                    }
                    while (idx <= limit);

                    h32 = Bits.RotateLeft32(v1, 1) + Bits.RotateLeft32(v2, 7) + Bits.RotateLeft32(v3, 12) + Bits.RotateLeft32(v4, 18);
                }
                else
                {
                    h32 = seed + XXHash32Constants.PRIME32_5;
                }

                h32 += (uint)buffer.Length;

                while (idx + 1 <= uintBuffer.Length)
                {
                    h32 += uintBuffer[idx] * XXHash32Constants.PRIME32_3;
                    h32 = Bits.RotateLeft32(h32, 17) * XXHash32Constants.PRIME32_4;
                    idx += 1;
                }

                idx *= 4;

                while (idx < buffer.Length)
                {
                    h32 += buffer[idx] * XXHash32Constants.PRIME32_5;
                    h32 = Bits.RotateLeft32(h32, 11) * XXHash32Constants.PRIME32_1;
                    idx++;
                }

                h32 ^= h32 >> 15;
                h32 *= XXHash32Constants.PRIME32_2;
                h32 ^= h32 >> 13;
                h32 *= XXHash32Constants.PRIME32_3;
                h32 ^= h32 >> 16;

                return h32;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static uint CalculateInline(byte* buffer, int len, uint seed = 0)
            {
                unchecked
                {
                    uint h32;

                    byte* bEnd = buffer + len;

                    if (len >= 16)
                    {
                        byte* limit = bEnd - 16;

                        uint v1 = seed + XXHash32Constants.PRIME32_1 + XXHash32Constants.PRIME32_2;
                        uint v2 = seed + XXHash32Constants.PRIME32_2;
                        uint v3 = seed + 0;
                        uint v4 = seed - XXHash32Constants.PRIME32_1;

                        do
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
                        }
                        while (buffer <= limit);

                        h32 = Bits.RotateLeft32(v1, 1) + Bits.RotateLeft32(v2, 7) + Bits.RotateLeft32(v3, 12) + Bits.RotateLeft32(v4, 18);
                    }
                    else
                    {
                        h32 = seed + XXHash32Constants.PRIME32_5;
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

            public static uint Calculate(byte* buffer, int len, uint seed = 0)
            {
                return CalculateInline(buffer, len, seed);
            }

            public static uint Calculate(string value, Encoding encoder, uint seed = 0)
            {
                var buf = encoder.GetBytes(value);

                fixed (byte* buffer = buf)
                {
                    return CalculateInline(buffer, buf.Length, seed);
                }
            }

            public static uint CalculateRaw(string buffer, uint seed = 0)
            {
                fixed (char* bufferPtr = buffer)
                {
                    return CalculateInline((byte*)bufferPtr, buffer.Length * sizeof(char), seed);
                }
            }

            public static uint Calculate(string buffer, uint seed = 0)
            {
                return CalculateInline<OrdinalModifier>(buffer, seed);
            }

            public static uint Calculate<TCharacterModifier>(string buffer, uint seed = 0) where TCharacterModifier : struct, ICharacterModifier
            {
                return CalculateInline<TCharacterModifier>(buffer, seed);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]            
            public static uint CalculateInline<TCharacterModifier>(string buffer, uint seed = 0) where TCharacterModifier : struct, ICharacterModifier
            {                
                unchecked
                {
                    uint h32;

                    uint len = (uint)buffer.Length;

                    uint position = 0;
                    if (len >= 8)
                    {                        
                        uint v1 = seed + XXHash32Constants.PRIME32_1 + XXHash32Constants.PRIME32_2;
                        uint v2 = seed + XXHash32Constants.PRIME32_2;
                        uint v3 = seed + 0;
                        uint v4 = seed - XXHash32Constants.PRIME32_1;

                        uint limit = len - 8;
                        do
                        {
                            v1 += CharToUInt32<TCharacterModifier>(buffer, position) * XXHash32Constants.PRIME32_2;
                            v2 += CharToUInt32<TCharacterModifier>(buffer, position + 2) * XXHash32Constants.PRIME32_2;
                            v3 += CharToUInt32<TCharacterModifier>(buffer, position + 4) * XXHash32Constants.PRIME32_2;
                            v4 += CharToUInt32<TCharacterModifier>(buffer, position + 6) * XXHash32Constants.PRIME32_2;

                            position += 8;

                            v1 = Bits.RotateLeft32(v1, 13);
                            v2 = Bits.RotateLeft32(v2, 13);
                            v3 = Bits.RotateLeft32(v3, 13);
                            v4 = Bits.RotateLeft32(v4, 13);

                            v1 *= XXHash32Constants.PRIME32_1;
                            v2 *= XXHash32Constants.PRIME32_1;
                            v3 *= XXHash32Constants.PRIME32_1;
                            v4 *= XXHash32Constants.PRIME32_1;
                        }
                        while (position <= limit);

                        h32 = Bits.RotateLeft32(v1, 1) + Bits.RotateLeft32(v2, 7) + Bits.RotateLeft32(v3, 12) + Bits.RotateLeft32(v4, 18);
                    }
                    else
                    {
                        h32 = seed + XXHash32Constants.PRIME32_5;
                    }

                    h32 += len * sizeof(char);

                    while (position + 2 <= len)
                    {
                        h32 += CharToUInt32<TCharacterModifier>(buffer, position) * XXHash32Constants.PRIME32_3;
                        h32 = Bits.RotateLeft32(h32, 17) * XXHash32Constants.PRIME32_4;
                        position += 2;
                    }

                    if (position != len)
                    {
                        h32 += buffer[(int)position] * XXHash32Constants.PRIME32_3;
                        h32 = Bits.RotateLeft32(h32, 17) * XXHash32Constants.PRIME32_4;
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
            private static uint CharToUInt32<TCharacterModifier>(string buffer, uint position) where TCharacterModifier : struct, ICharacterModifier
            {
                TCharacterModifier modifier = default(TCharacterModifier);
                return (uint)modifier.Modify(buffer[(int)position + 1]) << 16 | modifier.Modify(buffer[(int)position]);
            }

            public static uint Calculate(byte[] buf, int len = -1, uint seed = 0)
            {
                if (len == -1)
                    len = buf.Length;

                fixed (byte* buffer = buf)
                {
                    return CalculateInline(buffer, len, seed);
                }
            }

            public static uint Calculate(int[] buf, int len = -1, uint seed = 0)
            {
                if (len == -1)
                    len = buf.Length;

                fixed (int* buffer = buf)
                {
                    return Calculate((byte*)buffer, len * sizeof(int), seed);
                }
            }
        }

        public struct XXHash64Values
        {
            public ulong V1;
            public ulong V2;
            public ulong V3;
            public ulong V4;
        }

        internal static class XXHash64Constants
        {
            internal const ulong PRIME64_1 = 11400714785074694791UL;
            internal const ulong PRIME64_2 = 14029467366897019727UL;
            internal const ulong PRIME64_3 = 1609587929392839161UL;
            internal const ulong PRIME64_4 = 9650029242287828579UL;
            internal const ulong PRIME64_5 = 2870177450012600261UL;
        }

        public static class JumpConsistentHash
        {
            //A Fast, Minimal Memory, Consistent Hash Algorithm
            //by John Lamping, Eric Veach
            //relevant article: https://arxiv.org/abs/1406.2294
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static long Calculate(ulong key, int numBuckets)
            {
                long b = 1L;
                long j = 0;
                while (j < numBuckets)
                {
                    b = j;
                    key = key * 2862933555777941757UL + 1;
                    j = (long)((b + 1) * ((1L << 31) / ((double)(key >> 33) + 1)));
                }
                return b;
            }
        }

        /// <summary>
        /// A port of the original XXHash algorithm from Google in 64bits 
        /// </summary>
        /// <remarks>The 32bits and 64bits hashes for the same data are different. In short those are 2 entirely different algorithms</remarks>
        public static class XXHash64
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static ulong CalculateInline(byte* buffer, ulong len, ulong seed = 0)
            {
                ulong h64;

                byte* bEnd = buffer + len;

                if (len >= 32)
                {
                    byte* limit = bEnd - 32;

                    ulong v1 = seed + XXHash64Constants.PRIME64_1 + XXHash64Constants.PRIME64_2;
                    ulong v2 = seed + XXHash64Constants.PRIME64_2;
                    ulong v3 = seed + 0;
                    ulong v4 = seed - XXHash64Constants.PRIME64_1;

                    do
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
                    }
                    while (buffer <= limit);

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
                    h64 = seed + XXHash64Constants.PRIME64_5;
                }

                h64 += (ulong)len;


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

                h64 ^= h64 >> 33;
                h64 *= XXHash64Constants.PRIME64_2;
                h64 ^= h64 >> 29;
                h64 *= XXHash64Constants.PRIME64_3;
                h64 ^= h64 >> 32;

                return h64;
            }

            public static unsafe ulong Calculate(byte* buffer, ulong len, ulong seed = 0)
            {
                return CalculateInline(buffer, len, seed);
            }

            public static ulong Calculate(string value, Encoding encoder, ulong seed = 0)
            {
                var buf = encoder.GetBytes(value);

                fixed (byte* buffer = buf)
                {
                    return CalculateInline(buffer, (ulong)buf.Length, seed);
                }
            }

            public static ulong Calculate(string value, UTF8Encoding encoder, ulong seed = 0)
            {
                var buf = encoder.GetBytes(value);

                fixed (byte* buffer = buf)
                {
                    return CalculateInline(buffer, (ulong)buf.Length, seed);
                }
            }

            public static ulong CalculateRaw(string buf, ulong seed = 0)
            {
                fixed (char* buffer = buf)
                {
                    return CalculateInline((byte*)buffer, (ulong)(buf.Length * sizeof(char)), seed);
                }
            }

            public static ulong Calculate(ReadOnlySpan<byte> buf, int len = -1, ulong seed = 0)
            {
                if (len == -1)
                    len = buf.Length;

                fixed (byte* buffer = buf)
                {
                    return CalculateInline(buffer, (ulong)len, seed);
                }
            }

            public static ulong Calculate(byte[] buf, int len = -1, ulong seed = 0)
            {
                if (len == -1)
                    len = buf.Length;

                fixed (byte* buffer = buf)
                {
                    return CalculateInline(buffer, (ulong)len, seed);
                }
            }

            public static ulong Calculate(int[] buf, int len = -1, ulong seed = 0)
            {
                if (len == -1)
                    len = buf.Length;

                fixed (int* buffer = buf)
                {
                    return CalculateInline((byte*)buffer, (ulong)(len * sizeof(int)), seed);
                }
            }
        }

        #endregion

        #region Downsampling Hashing

        public static class HashCombiner
        {
            /// <summary>
            /// The combine function will perform the mixing of 2 hash values into a single one.
            /// It is important that the parameters are hash functions. If they are not the statistical properties of the output will be quite bad.
            /// For non hashes use <see cref="Hashing.Combine"/> instead
            /// </summary>
            /// <param name="x">The first hash value</param>
            /// <param name="y">The second hash value</param>
            /// <returns>The combined hash value</returns>
            public static int Combine(int x, int y)
            {
                return CombineInline(x, y);
            }

            /// <summary>
            /// The combine function will perform the mixing of 2 hash values into a single one.
            /// It is important that the parameters are hash functions. If they are not the statistical properties of the output will be quite bad.
            /// For non hashes use <see cref="Hashing.Combine"/> instead
            /// </summary>
            /// <param name="x">The first hash value</param>
            /// <param name="y">The second hash value</param>
            /// <returns>The combined hash value</returns>
            public static uint Combine(uint x, uint y)
            {
                return CombineInline(x, y);
            }

            /// <summary>
            /// The combine function will perform the mixing of 2 hash values into a single one.
            /// It is important that the parameters are hash functions. If they are not the statistical properties of the output will be quite bad.
            /// For non hashes use <see cref="Hashing.Combine"/> instead
            /// </summary>
            /// <param name="x">The first hash value</param>
            /// <param name="y">The second hash value</param>
            /// <returns>The combined hash value</returns>
            public static long Combine(long x, long y)
            {
                return CombineInline(x, y);
            }

            /// <summary>
            /// The combine function will perform the mixing of 2 hash values into a single one.
            /// It is important that the parameters are hash functions. If they are not the statistical properties of the output will be quite bad.
            /// For non hashes use <see cref="Hashing.Combine"/> instead
            /// </summary>
            /// <param name="x">The first hash value</param>
            /// <param name="y">The second hash value</param>
            /// <returns>The combined hash value</returns>
            public static ulong Combine(ulong x, ulong y)
            {
                return CombineInline(x, y);
            }

            /// <summary>
            /// The combine function will perform the mixing of 2 hash values into a single one.
            /// It is important that the parameters are hash functions. If they are not the statistical properties of the output will be quite bad.
            /// For non hashes use <see cref="Hashing.Combine"/> instead
            /// </summary>
            /// <remarks>This version will force inlining on the call-site</remarks>
            /// <param name="x">The first hash value</param>
            /// <param name="y">The second hash value</param>
            /// <returns>The combined hash value</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static int CombineInline(int x, int y)
            {
                // The jit optimizes this to use the ROL instruction on x86
                // Related GitHub pull request: dotnet/coreclr#1830
                uint shift5 = ((uint)x << 5) | ((uint)x >> 27);
                return ((int)shift5 + x) ^ y;
            }

            /// <summary>
            /// The combine function will perform the mixing of 2 hash values into a single one.
            /// It is important that the parameters are hash functions. If they are not the statistical properties of the output will be quite bad.
            /// For non hashes use <see cref="Hashing.Combine"/> instead
            /// </summary>
            /// <remarks>This version will force inlining on the call-site</remarks>
            /// <param name="x">The first hash value</param>
            /// <param name="y">The second hash value</param>
            /// <returns>The combined hash value</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static uint CombineInline(uint x, uint y)
            {
                // The jit optimizes this to use the ROL instruction on x86
                // Related GitHub pull request: dotnet/coreclr#1830
                uint shift5 = (x << 5) | (x >> 27);
                return (shift5 + x) ^ y;
            }

            /// <summary>
            /// The combine function will perform the mixing of 2 hash values into a single one.
            /// It is important that the parameters are hash functions. If they are not the statistical properties of the output will be quite bad.
            /// For non hashes use <see cref="Hashing.Combine"/> instead
            /// </summary>
            /// <remarks>This version will force inlining on the call-site</remarks>
            /// <param name="x">The first hash value</param>
            /// <param name="y">The second hash value</param>
            /// <returns>The combined hash value</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static long CombineInline(long x, long y)
            {
                // The jit optimizes this to use the ROL instruction on x86
                // Related GitHub pull request: dotnet/coreclr#1830
                ulong ux = (ulong)x;
                ulong shift5 = (ux << 10) | (ux >> 54);
                return (long) (shift5 + ux) ^ y;
            }

            /// <summary>
            /// The combine function will perform the mixing of 2 hash values into a single one.
            /// It is important that the parameters are hash functions. If they are not the statistical properties of the output will be quite bad.
            /// For non hashes use <see cref="Hashing.Combine"/> instead
            /// </summary>
            /// <remarks>This version will force inlining on the call-site</remarks>
            /// <param name="x">The first hash value</param>
            /// <param name="y">The second hash value</param>
            /// <returns>The combined hash value</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static ulong CombineInline(ulong x, ulong y)
            {
                // The jit optimizes this to use the ROL instruction on x86
                // Related GitHub pull request: dotnet/coreclr#1830
                ulong shift5 = (x << 10) | (x >> 54);
                return (shift5 + x) ^ y;
            }
        }

        private static readonly ulong kMul = 0x9ddfea08eb382d69UL;


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong Combine(ulong x, ulong y)
        {
            // This is the Hash128to64 function from Google's CityHash (available
            // under the MIT License).  We use it to reduce multiple 64 bit hashes
            // into a single hash.

            // Murmur-inspired hashing.
            ulong a = (y ^ x) * kMul;
            a ^= (a >> 47);
            ulong b = (x ^ a) * kMul;
            b ^= (b >> 47);
            b *= kMul;

            return b;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long Combine(long upper, long lower)
        {
            // This is the Hash128to64 function from Google's CityHash (available
            // under the MIT License).  We use it to reduce multiple 64 bit hashes
            // into a single hash.

            ulong x = (ulong)upper;
            ulong y = (ulong)lower;

            // Murmur-inspired hashing.
            ulong a = (y ^ x) * kMul;
            a ^= (a >> 47);
            ulong b = (x ^ a) * kMul;
            b ^= (b >> 47);
            b *= kMul;

            return (long)b;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint Mix(uint key)
        {
            key = ~key + (key << 15); // key = (key << 15) - key - 1;
            key = key ^ (key >> 12);
            key = key + (key << 2);
            key = key ^ (key >> 4);
            key = key * 2057; // key = (key + (key << 3)) + (key << 11);
            key = key ^ (key >> 16);
            return key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Mix(int key)
        {
            return (int)Mix((uint)key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint Mix(ulong key)
        {
            key = (~key) + (key << 18); // key = (key << 18) - key - 1;
            key = key ^ (key >> 31);
            key = key * 21; // key = (key + (key << 2)) + (key << 4);
            key = key ^ (key >> 11);
            key = key + (key << 6);
            key = key ^ (key >> 22);
            return (uint)key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Mix(long key)
        {
            key = (~key) + (key << 18); // key = (key << 18) - key - 1;
            key = key ^ (key >> 31);
            key = key * 21; // key = (key + (key << 2)) + (key << 4);
            key = key ^ (key >> 11);
            key = key + (key << 6);
            key = key ^ (key >> 22);
            return (int)key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint Combine(uint upper, uint lower)
        {
            ulong key = ((ulong)upper << 32) | lower;

            key = (~key) + (key << 18); // key = (key << 18) - key - 1;
            key = key ^ (key >> 31);
            key = key * 21; // key = (key + (key << 2)) + (key << 4);
            key = key ^ (key >> 11);
            key = key + (key << 6);
            key = key ^ (key >> 22);
            return (uint)key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Combine(int upper, int lower)
        {
            ulong x = (uint)upper; // Ensure we do not mess up with the sign extend.
            ulong key = (x << 32) | (uint)lower;

            key = (~key) + (key << 18); // key = (key << 18) - key - 1;
            key = key ^ (key >> 31);
            key = key * 21; // key = (key + (key << 2)) + (key << 4);
            key = key ^ (key >> 11);
            key = key + (key << 6);
            key = key ^ (key >> 22);
            return (int)key;
        }

        #endregion

        #region Metro128

        public struct Metro128Values
        {
            public ulong V0;
            public ulong V1;
            public ulong V2;
            public ulong V3;
        }

        internal static class Metro128Constants
        {                       
            public const ulong K0 = 0xC83A91E1;
            public const ulong K1 = 0x8648DBDB;
            public const ulong K2 = 0x7BDEC03B;
            public const ulong K3 = 0x2F5870A5;
        }

        public static class Metro128
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static unsafe Metro128Hash CalculateInline(byte* buffer, int length, ulong seed = 0)
            {
                byte* ptr = buffer;
                byte* end = ptr + length;

                ulong v0 = (seed - Metro128Constants.K0) * Metro128Constants.K3;
                ulong v1 = (seed + Metro128Constants.K1) * Metro128Constants.K2;

                if (length >= 32)
                {
                    ulong v2 = (seed + Metro128Constants.K0) * Metro128Constants.K2;
                    ulong v3 = (seed - Metro128Constants.K1) * Metro128Constants.K3;

                    do
                    {
                        v0 += ((ulong*)ptr)[0] * Metro128Constants.K0;
                        v1 += ((ulong*)ptr)[1] * Metro128Constants.K1;

                        v0 = Bits.RotateRight64(v0, 29) + v2;
                        v1 = Bits.RotateRight64(v1, 29) + v3;

                        v2 += ((ulong*)ptr)[2] * Metro128Constants.K2;
                        v3 += ((ulong*)ptr)[3] * Metro128Constants.K3;

                        v2 = Bits.RotateRight64(v2, 29) + v0;
                        v3 = Bits.RotateRight64(v3, 29) + v1;

                        ptr += 4 * sizeof(ulong);
                    }
                    while (ptr <= (end - 32));

                    v2 ^= Bits.RotateRight64(((v0 + v3) * Metro128Constants.K0) + v1, 21) * Metro128Constants.K1;
                    v3 ^= Bits.RotateRight64(((v1 + v2) * Metro128Constants.K1) + v0, 21) * Metro128Constants.K0;
                    v0 ^= Bits.RotateRight64(((v0 + v2) * Metro128Constants.K0) + v3, 21) * Metro128Constants.K1;
                    v1 ^= Bits.RotateRight64(((v1 + v3) * Metro128Constants.K1) + v2, 21) * Metro128Constants.K0;
                }

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

                v0 += Bits.RotateRight64((v0 * Metro128Constants.K0) + v1, 13);
                v1 += Bits.RotateRight64((v1 * Metro128Constants.K1) + v0, 37);
                v0 += Bits.RotateRight64((v0 * Metro128Constants.K2) + v1, 13);
                v1 += Bits.RotateRight64((v1 * Metro128Constants.K3) + v0, 37);

                return new Metro128Hash { H1 = v0, H2 = v1 };
            }

            public static unsafe Metro128Hash Calculate(byte* buffer, int len, ulong seed = 0)
            {
                return CalculateInline(buffer, len, seed);
            }

            public static Metro128Hash Calculate(string value, Encoding encoder, ulong seed = 0)
            {
                var buf = encoder.GetBytes(value);

                fixed (byte* buffer = buf)
                {
                    return CalculateInline(buffer, buf.Length, seed);
                }
            }

            public static Metro128Hash CalculateRaw(string buf, ulong seed = 0)
            {
                fixed (char* buffer = buf)
                {
                    return CalculateInline((byte*)buffer, buf.Length * sizeof(char), seed);
                }
            }

            public static Metro128Hash Calculate(byte[] buf, int len = -1, ulong seed = 0)
            {
                if (len == -1)
                    len = buf.Length;

                fixed (byte* buffer = buf)
                {
                    return CalculateInline(buffer, len, seed);
                }
            }

            public static Metro128Hash Calculate(int[] buf, int len = -1, ulong seed = 0)
            {
                if (len == -1)
                    len = buf.Length;

                fixed (int* buffer = buf)
                {
                    return CalculateInline((byte*)buffer, len * sizeof(int), seed);
                }
            }
        }

        #endregion

        public static class Marvin32
        {
            public static uint Calculate(byte[] buffer, ulong seed = 0x5D70D359C498B3F8ul)
            {
                fixed ( byte* ptr = buffer )
                    return CalculateInline(ptr, buffer.Length, seed);
            }

            public static uint Calculate(string buffer, ulong seed = 0x5D70D359C498B3F8ul)
            {
                return CalculateInline<OrdinalModifier>(buffer, seed);
            }

            public static uint Calculate<TCharacterModifier>(string buffer, ulong seed = 0x5D70D359C498B3F8ul) where TCharacterModifier : struct, ICharacterModifier
            {
                return CalculateInline<TCharacterModifier>(buffer, seed);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static uint CalculateInline(int[] buffer, ulong seed = 0x5D70D359C498B3F8ul)
            {
                uint high = (uint)(seed >> 32);
                uint low = (uint)seed;

                for (uint i = 0; i < buffer.Length; i++)
                {
                    MarvinMix(ref high, ref low, (uint)buffer[i]);
                }

                MarvinMix(ref high, ref low, 0x80);
                MarvinMix(ref high, ref low, 0);

                return low ^ high;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static uint CalculateInline(uint[] buffer, ulong seed = 0x5D70D359C498B3F8ul)
            {
                uint high = (uint)(seed >> 32);
                uint low = (uint)seed;

                for (uint i = 0; i < buffer.Length; i++)
                {
                    MarvinMix(ref high, ref low, (uint)buffer[i]);
                }

                MarvinMix(ref high, ref low, 0x80);
                MarvinMix(ref high, ref low, 0);

                return low ^ high;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static uint CalculateInline(List<uint> buffer, ulong seed = 0x5D70D359C498B3F8ul)
            {
                uint high = (uint)(seed >> 32);
                uint low = (uint)seed;

                int len = buffer.Count;
                for (int i = 0; i < len; i++)
                {
                    MarvinMix(ref high, ref low, buffer[i]);
                }

                MarvinMix(ref high, ref low, 0x80);
                MarvinMix(ref high, ref low, 0);

                return low ^ high;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static uint CalculateInline(List<int> buffer, ulong seed = 0x5D70D359C498B3F8ul)
            {
                uint high = (uint)(seed >> 32);
                uint low = (uint)seed;

                int len = buffer.Count;
                for (int i = 0; i < len; i++)
                {
                    MarvinMix(ref high, ref low, (uint)buffer[i]);
                }

                MarvinMix(ref high, ref low, 0x80);
                MarvinMix(ref high, ref low, 0);

                return low ^ high;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static uint CalculateInline(byte* buffer, int len, ulong seed = 0x5D70D359C498B3F8ul)
            {
                uint high = (uint)(seed >> 32);
                uint low = (uint)seed;

                byte* ptr = buffer;
                byte* bEnd = ptr + len;
                byte* loopEnd = bEnd - sizeof(uint);

                while (ptr <= loopEnd)
                {
                    MarvinMix(ref high, ref low, *(uint*)ptr);
                    ptr += sizeof(uint);
                }

                uint final = 0x80;
                int rest = (int)(bEnd - ptr);
                if (rest == 3)
                    final = (final << 8) | ptr[2];
                if (rest >= 2)
                    final = (final << 8) | ptr[1];
                if (rest >= 1)
                    final = (final << 8) | ptr[0];

                MarvinMix(ref high, ref low, final);
                MarvinMix(ref high, ref low, 0);

                return low ^ high;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static uint CalculateInline(string buffer, ulong seed = 0x5D70D359C498B3F8ul)
            {
                return CalculateInline<OrdinalModifier>(buffer, seed);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static uint CalculateInline<TCharacterModifier>(string buffer, ulong seed = 0x5D70D359C498B3F8ul) where TCharacterModifier : struct, ICharacterModifier
            {
                uint high = (uint)(seed >> 32);
                uint low = (uint)seed;

                int len = buffer.Length;
                int len2 = len - 2;
                int position = 0;
                while (position <= len2)
                {
                    MarvinMix(ref high, ref low, CharToUInt32<TCharacterModifier>(buffer, position));
                    position += 2;
                }

                uint final = 0x80;
                if ((len & 1) != 0) // Case we have yet another char available to process.
                {
                    TCharacterModifier modifier = default(TCharacterModifier);
                    final = (final << 16) | modifier.Modify(buffer[position]);
                }

                MarvinMix(ref high, ref low, final);
                MarvinMix(ref high, ref low, 0);

                return low ^ high;
            }
            
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static uint CalculateInline<TCharacterModifier>(ReadOnlySpan<char> buffer, ulong seed = 0x5D70D359C498B3F8ul) where TCharacterModifier : struct, ICharacterModifier
            {
                uint high = (uint)(seed >> 32);
                uint low = (uint)seed;

                int len = buffer.Length;
                int len2 = len - 2;
                int position = 0;
                while (position <= len2)
                {
                    MarvinMix(ref high, ref low, CharToUInt32<TCharacterModifier>(buffer, position));
                    position += 2;
                }

                uint final = 0x80;
                if ((len & 1) != 0) // Case we have yet another char available to process.
                {
                    TCharacterModifier modifier = default(TCharacterModifier);
                    final = (final << 16) | modifier.Modify(buffer[position]);
                }

                MarvinMix(ref high, ref low, final);
                MarvinMix(ref high, ref low, 0);

                return low ^ high;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static uint CharToUInt32<TCharacterModifier>(string buffer, int position) where TCharacterModifier : struct, ICharacterModifier
            {
                TCharacterModifier modifier = default(TCharacterModifier);
                return (uint)modifier.Modify(buffer[position + 1]) << 16 | modifier.Modify(buffer[position]);
            }
            
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static uint CharToUInt32<TCharacterModifier>(ReadOnlySpan<char> buffer, int position) where TCharacterModifier : struct, ICharacterModifier
            {
                TCharacterModifier modifier = default(TCharacterModifier);
                return (uint)modifier.Modify(buffer[position + 1]) << 16 | modifier.Modify(buffer[position]);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static void MarvinMix(ref uint high, ref uint low, uint v)
            {
                low += v;
                high ^= low;
                low = Bits.RotateLeft32(low, 20) + high;
                high = Bits.RotateLeft32(high, 9) ^ low;
                low = Bits.RotateLeft32(low, 27) + high;
                high = Bits.RotateLeft32(high, 19);
            }
        }

    }
}
