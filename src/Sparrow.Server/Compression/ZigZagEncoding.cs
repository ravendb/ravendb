using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Server.Compression
{
    public static class ZigZagEncoding
    {
        public const int MaxEncodedSize = 10;

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static int Encode<T>(Span<byte> buffer, T value, int pos = 0) where T : unmanaged
        {
            int sizeOfTInBits = Unsafe.SizeOf<T>() * 8 - 1;
            if (typeof(T) == typeof(sbyte))
            {
                byte b;
                if (typeof(T) == typeof(bool))
                    b = (bool)(object)value ? (byte)1 : (byte)0;
                else if (typeof(T) == typeof(sbyte))
                    b = (byte)(sbyte)(object)value;
                else
                    b = (byte)(object)value;

                byte uv = (byte)((b << 1) ^ (b >> sizeOfTInBits));
                return VariableSizeEncoding.Write(buffer, uv, pos);
            }

            if (typeof(T) == typeof(short))
            {
                ushort us;
                if (typeof(T) == typeof(short))
                    us = (ushort)(short)(object)value;
                else
                    us = (ushort)(object)value;

                ushort uv = (ushort)((us << 1) ^ (us >> sizeOfTInBits));
                return VariableSizeEncoding.Write(buffer, uv, pos);
            }

            if (typeof(T) == typeof(int))
            {
                uint ui;
                if (typeof(T) == typeof(int))
                    ui = (uint)(int)(object)value;
                else
                    ui = (uint)(object)value;

                uint uv = ((ui << 1) ^ (ui >> sizeOfTInBits));
                return VariableSizeEncoding.Write(buffer, uv, pos);
            }

            if (typeof(T) == typeof(long))
            {
                long ul = (long)(object)value;

                ulong uv = (ulong)((ul << 1) ^ (ul >> sizeOfTInBits));
                return VariableSizeEncoding.Write(buffer, uv, pos);
            }

            throw new NotSupportedException($"The type {nameof(T)} is not supported to be written.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static unsafe int Encode<T>(byte* buffer, T value, int pos = 0) where T : unmanaged
        {
            int sizeOfTInBits = Unsafe.SizeOf<T>() * 8 - 1;
            if (typeof(T) == typeof(sbyte))
            {
                byte b;
                if (typeof(T) == typeof(bool))
                    b = (bool)(object)value ? (byte)1 : (byte)0;
                else if (typeof(T) == typeof(sbyte))
                    b = (byte)(sbyte)(object)value;
                else
                    b = (byte)(object)value;

                byte uv = (byte)((b << 1) ^ (b >> sizeOfTInBits));
                return VariableSizeEncoding.Write(buffer + pos, uv);
            }

            if (typeof(T) == typeof(short))
            {
                ushort us;
                if (typeof(T) == typeof(short))
                    us = (ushort)(short)(object)value;
                else
                    us = (ushort)(object)value;

                ushort uv = (ushort)((us << 1) ^ (us >> sizeOfTInBits));
                return VariableSizeEncoding.Write(buffer + pos, uv);
            }

            if (typeof(T) == typeof(int))
            {
                uint ui;
                if (typeof(T) == typeof(int))
                    ui = (uint)(int)(object)value;
                else
                    ui = (uint)(object)value;

                uint uv = ((ui << 1) ^ (ui >> sizeOfTInBits));
                return VariableSizeEncoding.Write(buffer + pos, uv);
            }

            if (typeof(T) == typeof(long))
            {
                long ul = (long)(object)value;

                ulong uv = (ulong)((ul << 1) ^ (ul >> sizeOfTInBits));
                return VariableSizeEncoding.Write(buffer + pos, uv);
            }

            throw new NotSupportedException($"The type {nameof(T)} is not supported to be written.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static T Decode<T>(ReadOnlySpan<byte> buffer, int pos = 0) where T : unmanaged
        {
            T value = VariableSizeEncoding.Read<T>(buffer, out int _, pos);
            return UnZag(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static unsafe T Decode<T>(byte* buffer, out int len, int pos = 0) where T : unmanaged
        {
            T value = VariableSizeEncoding.Read<T>(buffer + pos, out len);
            return UnZag(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static unsafe T DecodeCompact<T>(byte* buffer, out int len, out bool success, int pos = 0) where T : unmanaged
        {
            T value = VariableSizeEncoding.ReadCompact<T>(buffer + pos, out len, out success);
            return UnZag(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static T Decode<T>(ReadOnlySpan<byte> buffer, out int len, int pos = 0) where T : unmanaged
        {
            T value = VariableSizeEncoding.Read<T>(buffer, out len, pos);
            return UnZag(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static long[] DecodeDebug(ReadOnlySpan<byte> buffer) 
        {
            int count = UnZag(VariableSizeEncoding.Read<int>(buffer, out var len, 0));
            var results = new long[count];
            var pos = len;
            long cur = 0;
            for (int i = 0; i < count; i++)
            {
                cur += Decode<long>(buffer, out len, pos);
                results[i] = cur;
                pos += len;
            }
            return results;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static T UnZag<T>(T value) where T : unmanaged
        {
            if (typeof(T) == typeof(sbyte))
            {
                var b = (byte)(sbyte)(object)value;
                return (T)(object)((b & 1) != 0 ? (sbyte)(b >> 1) ^ -1 : (sbyte)(b >> 1));
            }

            if (typeof(T) == typeof(short))
            {
                var us = (ushort)(short)(object)value;
                return (T)(object)((us & 1) != 0 ? (short)(us >> 1) ^ -1 : (short)(us >> 1));
            }

            if (typeof(T) == typeof(int))
            {
                var ui = (uint)(int)(object)value;
                return (T)(object)((ui & 1) != 0 ? (int)(ui >> 1) ^ -1 : (int)(ui >> 1));
            }

            if (typeof(T) == typeof(long))
            {
                var ul = (ulong)(long)(object)value;
                return (T)(object)((ul & 1) != 0 ? (long)(ul >> 1) ^ -1 : (long)(ul >> 1));
            }

            throw new NotSupportedException($"The type {nameof(T)} is not supported to be read.");           
        }

        public static int SizeOf2<T>(ReadOnlySpan<byte> buffer, int pos = 0) where T : unmanaged
        {
            VariableSizeEncoding.Read<T>(buffer, out int fst, pos);
            VariableSizeEncoding.Read<T>(buffer, out int snd, pos + fst);
            return fst + snd;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static (T, T) Decode2<T>(ReadOnlySpan<byte> buffer, out int len, int pos = 0) where T : unmanaged
        {
            T one = UnZag(VariableSizeEncoding.Read<T>(buffer, out int fst, pos));
            T two = UnZag(VariableSizeEncoding.Read<T>(buffer, out int snd, pos + fst));

            len = fst + snd;
            return (one, two);
        }
    }
}
