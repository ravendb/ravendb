using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Binary;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Platform.Win32;
using Voron.Platform.Posix;

namespace Sparrow
{
    public static unsafe class Memory
    {
        public const int CompareInlineVsCallThreshold = 256;

        public static int Compare(byte* p1, byte* p2, int size)
        {
            return CompareInline(p1, p2, size);
        }

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
            return UnmanagedMemory.Compare((byte*)p1, (byte*)p2, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(void* p1, void* p2, int size, out int position)
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

        /// <summary>
        /// Bulk copy is optimized to handle copy operations where n is statistically big. While it will use a faster copy operation for 
        /// small amounts of memory, when you have smaller than 2048 bytes calls (depending on the target CPU) it will always be
        /// faster to call .Copy() directly.
        /// </summary>
        
        private static void BulkCopy(void* dest, void* src, long n)
        {
            UnmanagedMemory.Copy((byte*)dest, (byte*)src, n);            
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(void* dest, void* src, uint n)
        {
            Unsafe.CopyBlock(dest, src, n);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(void* dest, void* src, int n)
        {
            Unsafe.CopyBlock(dest, src, (uint)n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(void* dest, void* src, long n)
        {
            if (n < uint.MaxValue)
            {
                Unsafe.CopyBlock(dest, src, (uint)n); // Common code-path
                return;
            }

            BulkCopy(dest, src, n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Set(byte* dest, byte value, uint n)
        {
            Unsafe.InitBlock(dest, value, n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Set(byte* dest, byte value, int n)
        {
            Unsafe.InitBlock(dest, value, (uint)n);
        }

        public static void Set(byte* dest, byte value, long n)
        {
            SetInline(dest, value, n);
        }

        /// <summary>
        /// Set is optimized to handle copy operations where n is statistically small.       
        /// </summary>
        /// <remarks>This is a forced inline version, use with care.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SetInline(byte* dest, byte value, long n)
        {
            if (n == 0)
                goto Finish;

            if (n < int.MaxValue)
            {
                Unsafe.InitBlock(dest, value, (uint)n);
            }
            else
            {
                UnmanagedMemory.Set(dest, value, n);
            }

            Finish:
            ;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Discard(void* baseAddress, long size)
        {
            if (PlatformDetails.CanDiscardMemory)
            {
                // We explicitely ignore the return codes because even if it fails, from all uses and purposes we dont care. 
                if (PlatformDetails.RunningOnPosix)
                {
                    Syscall.madvise(new IntPtr(baseAddress), new UIntPtr((ulong)size), MAdvFlags.MADV_DONTNEED);
                }
                else
                {
                    Win32MemoryProtectMethods.DiscardVirtualMemory(baseAddress, new UIntPtr((ulong)size));
                }
            }            
        }
    }
}
