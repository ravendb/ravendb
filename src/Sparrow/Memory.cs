using System;
using System.Runtime.CompilerServices;
using Sparrow.Binary;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Sparrow
{
    public static unsafe class Memory
    {
        private const int CompareInlineVsCallThreshold = 256;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Compare(byte* p1, byte* p2, int size)
        {
            return CompareInline(p1, p2, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Compare(byte* p1, byte* p2, int size, out int position)
        {
            return CompareInline(p1, p2, size, out position);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(void* p1, void* p2, int size)
        {
            // If we use an unmanaged bulk version with an inline compare the caller site does not get optimized properly.
            // If you know you will be comparing big memory chunks do not use the inline version. 
            if (size > CompareInlineVsCallThreshold)
                goto UnmanagedCompare;

            byte* bpx = (byte*)p1;
            byte* bpy = (byte*)p2;

            // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
            //       hardware instructions without needed an extra register.            
            long offset = bpy - bpx;

            if ((size & 7) == 0)
                goto ProcessAligned;

            // We process first the "unaligned" size.
            ulong xor;
            if ((size & 4) != 0)
            {
                xor = *((uint*)bpx) ^ *((uint*)(bpx + offset));
                if (xor != 0)
                    goto Tail;

                bpx += 4;
            }

            if ((size & 2) != 0)
            {
                xor = (ulong)(*((ushort*)bpx) ^ *((ushort*)(bpx + offset)));
                if (xor != 0)
                    goto Tail;

                bpx += 2;
            }

            if ((size & 1) != 0)
            {
                int value = *bpx - *(bpx + offset);
                if (value != 0)
                    return value;

                bpx += 1;
            }

            ProcessAligned:

            byte* end = (byte*)p1 + size;
            byte* loopEnd = end - 16;
            while (bpx <= loopEnd)
            {
                // PERF: JIT will emit: ```{op} {reg}, qword ptr [rdx+rax]```
                if (*((ulong*)bpx) != *(ulong*)(bpx + offset))
                    goto XorTail;

                if (*((ulong*)(bpx + 8)) != *(ulong*)(bpx + 8 + offset))
                {
                    bpx += 8;
                    goto XorTail;
                }
                   

                bpx += 16;
            }

            if (bpx < end)
                goto XorTail;

            return 0;

            XorTail: xor = *((ulong*)bpx) ^ *(ulong*)(bpx + offset);

            Tail:

            // Fast-path for equals
            if (xor == 0)
                return 0;

            // PERF: This is a bit twiddling hack. Given that bitwise xoring 2 values flag the bits difference, 
            //       we can use that we know we are running on little endian hardware and the very first bit set 
            //       will correspond to the first byte which is different. 

            bpx += Bits.TrailingZeroesInBytes(xor);
            return *bpx - *(bpx + offset);

UnmanagedCompare:
            return PlatformDetails.RunningOnPosix
                ? Syscall.Compare((byte *)p1, (byte *)p2, size)
                : Win32UnmanagedMemory.Compare((byte *)p1, (byte *)p2, size); 
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CompareInline(void* p1, void* p2, int size, out int position)
        {
            byte* bpx = (byte*)p1;
            byte* bpy = (byte*)p2;

            long offset = bpy - bpx;
            if (size < 8)
                goto ProcessSmall;

            int l = size >> 3; // (Equivalent to size / 8)

            ulong xor;
            for (int i = 0; i < l; i++, bpx += 8)
            {
                xor = *((ulong*)bpx) ^ *(ulong*)(bpx + offset);
                if (xor != 0)
                    goto Tail;
            }

            ProcessSmall:

            if ((size & 4) != 0)
            {
                xor = *((uint*)bpx) ^ *((uint*)(bpx + offset));
                if (xor != 0)
                    goto Tail;

                bpx += 4;
            }

            if ((size & 2) != 0)
            {
                xor = (ulong)(*((ushort*)bpx) ^ *((ushort*)(bpx + offset)));
                if (xor != 0)
                    goto Tail;

                bpx += 2;
            }

            position = (int)(bpx - (byte*)p1);

            if ((size & 1) != 0)
            {             
                return *bpx - *(bpx + offset);
            }

            return 0;

            Tail:

            int p = Bits.TrailingZeroesInBytes(xor);

            position = (int)(bpx - (byte*)p1) + p;
            return *(bpx + p) - *(bpx + p + offset);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(void* dest, void* src, uint n)
        {
            Unsafe.CopyBlock(dest, src, n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(void* dest, void* src, long n)
        {
            if (n < uint.MaxValue) // Common code-path
            {
                Copy(dest, src, (uint)n);
                return;
            }

            for (long i = 0; i < n; i += uint.MaxValue)
            {
                var size = uint.MaxValue;
                if (i + uint.MaxValue > n)
                    size = (uint)(n % uint.MaxValue);
                Copy((byte*)dest + i, (byte*)src + i, size);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Set(byte* dest, byte value, uint n)
        {
            Unsafe.InitBlock(dest, value, n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Set(byte* dest, byte value, long n)
        {
            if (n < uint.MaxValue) // Common code-path
            {
                Set(dest, value, (uint)n);
            }
            else
            {
                for (long i = 0; i < n; i += uint.MaxValue)
                {
                    var size = uint.MaxValue;
                    if (i + uint.MaxValue > n)
                        size = (uint)(n % uint.MaxValue);
                    Set(dest + i, value, size);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Move(byte* dest, byte* src, int n)
        {
            // if dest and src overlaps, we need to call specifically to memmove pinvoke supporting overlapping
            var d = new IntPtr(dest).ToInt64();
            var s = new IntPtr(src).ToInt64();
            if (d - n >= s &&
                s + n >= d)
            {
                var _ = PlatformDetails.RunningOnPosix
                    ? Syscall.Move(dest, src, n)
                    : Win32UnmanagedMemory.Move(dest, src, n);
                return;
            }

            Copy(dest, src, n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyUnaligned(byte* dest, byte* src, uint n)
        {
            Unsafe.CopyBlockUnaligned(dest, src, n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TTo As<TFrom, TTo>(ref TFrom value)
        {
            return Unsafe.As<TFrom, TTo>(ref value);            
        }

        public static T Read<T>(byte* ptr)
        {
            return Unsafe.Read<T>(ptr);
        }
    }
}
