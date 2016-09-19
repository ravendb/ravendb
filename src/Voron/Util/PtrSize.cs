using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;

namespace Voron.Util
{
    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct PtrSize
    {
        private const uint ValueMask = 0x80000000;

        [FieldOffset(0)]
        public readonly byte* Ptr;

        [FieldOffset(0)]
        public readonly long Value;

        [FieldOffset(8)]
        private readonly uint InternalSize;
            
        
        public int Size
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (int)(InternalSize & ~ValueMask); }
        }

        public bool IsValue
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (InternalSize & ValueMask) != 0; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private PtrSize(long value, uint size)
        {
            Debug.Assert(size <= 8);

            Ptr = null;
            Value = value;
            InternalSize = ValueMask | size;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private PtrSize(byte* ptr, uint size)
        {
            Debug.Assert(size <= int.MaxValue >> 1);

            Value = 0;
            Ptr = ptr;
            InternalSize = size;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PtrSize Create<T>(T value) where T : struct 
        {
            if (typeof(T) == typeof(ulong) || typeof(T) == typeof(long))
            {
                return new PtrSize((long)(object)value, sizeof(long));
            }

            if (typeof(T) == typeof(uint) || typeof(T) == typeof(int))
            {
                return new PtrSize((long)(object)value, sizeof(int));
            }

            if (typeof(T) == typeof(ushort) || typeof(T) == typeof(short))
            {
                return new PtrSize((long)(object)value, sizeof(short));
            }

            if (typeof(T) == typeof(bool) || typeof(T) == typeof(byte) || typeof(T) == typeof(sbyte))
            {
                return new PtrSize((long)(object)value, sizeof(byte));
            }

            throw new NotSupportedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PtrSize Create(byte* ptr, int size)
        {
            return new PtrSize(ptr, (uint)size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PtrSize Create(Slice value)
        {
            return new PtrSize(value.Content.Ptr, (uint)value.Size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PtrSize Create(ByteString value)
        {
            return new PtrSize(value.Ptr, (uint)value.Length);
        }
    }
}