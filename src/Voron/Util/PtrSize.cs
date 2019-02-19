using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Server;

namespace Voron.Util
{
    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct PtrSize
    {
        private const uint ValueMask = 0x80000000;

        [FieldOffset(0)]
        public readonly byte* Ptr;

        [FieldOffset(0)]
        public readonly ulong Value;

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
        private PtrSize(ulong value, uint size)
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

        /// <summary>
        /// Beware of touching this code mindlessly, the conversions done here
        /// were made taking into account endianness of values.
        /// </summary>
        /// <typeparam name="T">Type to create PtrSize of</typeparam>
        /// <param name="value">Value to insert</param>
        /// <returns>A PtrSize structure</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PtrSize Create<T>(T value) where T : struct 
        {
            if (typeof(T) == typeof(ulong))
            {
                return new PtrSize((ulong)(object)value, sizeof(ulong));
            }

            if (typeof(T) == typeof(long))
            {
                long v = (long)(object)value;
                return new PtrSize((ulong)v, sizeof(long));
            }

            if (typeof(T) == typeof(uint))
            {
                uint v = (uint)(object)value;
                return new PtrSize(v, sizeof(uint));
            }

            if (typeof(T) == typeof(int))
            {
                int v = (int)(object)value;
                return new PtrSize((ulong)v, sizeof(int));
            }

            if (typeof(T) == typeof(ushort))
            {
                ushort v = (ushort)(object)value;
                return new PtrSize(v, sizeof(ushort));
            }

            if (typeof(T) == typeof(short))
            {
                short v = (short)(object)value;
                return new PtrSize((ulong)v, sizeof(short));
            }

            if (typeof(T) == typeof(byte))
            {
                byte v = (byte)(object)value;
                return new PtrSize(v, sizeof(byte));
            }

            if (typeof(T) == typeof(sbyte))
            {
                sbyte v = (sbyte)(object)value;
                return new PtrSize((ulong)v, sizeof(sbyte));
            }

            if (typeof(T) == typeof(bool))
            {
                bool v = (bool) (object) value;
                return new PtrSize(v? (byte)1 : (byte)0, sizeof(byte));
            }

            if (typeof(T) == typeof(Slice))
            {
                var s = (Slice) (object) value;
                return new PtrSize(s.Content.Ptr, (uint)s.Size);
            }

            if (typeof(T) == typeof(ByteString))
            {
                var s = (ByteString) (object) value;
                return new PtrSize(s.Ptr, (uint)s.Length);
            }

            ThrowNotSupportedException();
            return default(PtrSize);
        }

        private static void ThrowNotSupportedException()
        {
            throw new NotSupportedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PtrSize Create(void* ptr, int size)
        {
            return new PtrSize((byte*)ptr, (uint)size);
        }
    }
}