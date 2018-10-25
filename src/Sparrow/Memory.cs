using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

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
            int l = size;
            if (l > CompareInlineVsCallThreshold)
                goto UnmanagedCompare;

            byte* bpx = (byte*)p1, bpy = (byte*)p2;
            int last;
            for (int i = 0; i < l / 8; i++, bpx += 8, bpy += 8)
            {
                if (*((long*)bpx) != *((long*)bpy))
                {
                    last = 8;
                    goto Tail;
                }
            }

            if ((l & 4) != 0)
            {
                if (*((int*)bpx) != *((int*)bpy))
                {
                    last = 4;
                    goto Tail;
                }
                bpx += 4;
                bpy += 4;
            }

            if ((l & 2) != 0)
            {
                if (*((short*)bpx) != *((short*)bpy))
                {
                    last = 2;
                    goto Tail;
                }

                bpx += 2;
                bpy += 2;
            }

            if ((l & 1) != 0)
            {
                return (*((byte*)bpx) - *((byte*)bpy));
            }

            return 0;

            Tail:
            while (last > 0)
            {
                if (*((byte*)bpx) != *((byte*)bpy))
                    return *bpx - *bpy;

                bpx++;
                bpy++;
                last--;
            }

            return 0;

            UnmanagedCompare:
            return UnmanagedMemory.Compare((byte*)p1, (byte*)p2, l);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(byte* p1, byte* p2, int size, out int position)
        {
            byte* bpx = p1, bpy = p2;
            int l = size;

            int last = 0;
            for (int i = 0; i < l / 8; i++, bpx += 8, bpy += 8)
            {
                if (*((long*)bpx) != *((long*)bpy))
                {
                    last = 8;
                    goto TAIL;
                }
            }

            if ((l & 4) != 0)
            {
                if (*((int*)bpx) != *((int*)bpy))
                {
                    last = 4;
                    goto TAIL;
                }
                bpx += 4;
                bpy += 4;
            }
            if ((l & 2) != 0)
            {
                if (*((short*)bpx) != *((short*)bpy))
                {
                    last = 2;
                    goto TAIL;
                }

                bpx += 2;
                bpy += 2;
            }

            if ((l & 1) != 0)
            {
                position = (int)(bpx - p1);
                return (*((byte*)bpx) - *((byte*)bpy));
            }

            position = size;
            return 0;

        TAIL:
            while (last > 0)
            {
                if (*((byte*)bpx) != *((byte*)bpy))
                {
                    position = (int)(bpx - p1);
                    return *bpx - *bpy;
                }
                
                bpx++;
                bpy++;
                last--;
            }

            position = size;
            return 0;
        }

        /// <summary>
        /// Bulk copy is optimized to handle copy operations where n is statistically big. While it will use a faster copy operation for 
        /// small amounts of memory, when you have smaller than 2048 bytes calls (depending on the target CPU) it will always be
        /// faster to call .Copy() directly.
        /// </summary>
        
        private static void BulkCopy(byte* dest, byte* src, long n)
        {
            UnmanagedMemory.Copy(dest, src, n);            
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(byte* dest, byte* src, uint n)
        {
            Debug.Assert(Math.Abs(dest - src) >= n, "overlapped copy using memcopy");

            Unsafe.CopyBlock(dest, src, n);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(byte* dest, byte* src, int n)
        {
            Debug.Assert(Math.Abs(dest-src) >= n, "overlapped copy using memcopy");

            Unsafe.CopyBlock(dest, src, (uint)n);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(byte* dest, byte* src, long n)
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
    }
}
