using DotNetCross.Memory;
using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Security;

namespace Sparrow
{
    public unsafe static class Memory
    {
        public readonly static int CompareInlineVsCallThreshold = 128;

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
            if ( l > CompareInlineVsCallThreshold)
            {
                if (size >= 256)
                    return UnmanagedMemory.Compare((byte*)p1, (byte*)p2, l);
            }

            byte* bpx = (byte*)p1, bpy = (byte*)p2;
            int last;
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
                return (*((byte*)bpx) - *((byte*)bpy));
            }

            return 0;

            TAIL:
            while (last > 0)
            {
                if (*((byte*)bpx) != *((byte*)bpy))
                    return *bpx - *bpy;

                bpx++;
                bpy++;
                last--;
            }

            return 0;
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void BulkCopy(byte* dest, byte* src, int n)
        {
            UnmanagedMemory.Copy(dest, src, n);            
        }

        public static void Copy(byte* dest, byte* src, int n)
        {
            CopyInline(dest, src, n);
        }

        /// <summary>
        /// Copy is optimized to handle copy operations where n is statistically small. 
        /// This method is optimized at the IL level to be extremely efficient for copies smaller than
        /// 4096 bytes or heterogeneous workloads with occasional big copies.         
        /// </summary>
        /// <remarks>This is a forced inline version, use with care.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe static void CopyInline(byte* dest, byte* src, int n)
        {
            if (n < 0)
                throw new ArgumentOutOfRangeException(nameof(n), "Cannot be less than zero");

            SMALLTABLE:
            switch (n)
            {
                case 16:
                    *(long*)dest = *(long*)src;
                    *(long*)(dest + 8) = *(long*)(src + 8);
                    return;
                case 15:
                    *(short*)(dest + 12) = *(short*)(src + 12);
                    *(dest + 14) = *(src + 14);
                    goto case 12;
                case 14:
                    *(short*)(dest + 12) = *(short*)(src + 12);
                    goto case 12;
                case 13:
                    *(dest + 12) = *(src + 12);
                    goto case 12;
                case 12:
                    *(long*)dest = *(long*)src;
                    *(int*)(dest + 8) = *(int*)(src + 8);
                    return;
                case 11:
                    *(short*)(dest + 8) = *(short*)(src + 8);
                    *(dest + 10) = *(src + 10);
                    goto case 8;
                case 10:
                    *(short*)(dest + 8) = *(short*)(src + 8);
                    goto case 8;
                case 9:
                    *(dest + 8) = *(src + 8);
                    goto case 8;
                case 8:
                    *(long*)dest = *(long*)src;
                    return;
                case 7:
                    *(short*)(dest + 4) = *(short*)(src + 4);
                    *(dest + 6) = *(src + 6);
                    goto case 4;
                case 6:
                    *(short*)(dest + 4) = *(short*)(src + 4);
                    goto case 4;
                case 5:
                    *(dest + 4) = *(src + 4);
                    goto case 4;
                case 4:
                    *(int*)dest = *(int*)src;
                    return;
                case 3:
                    *(dest + 2) = *(src + 2);
                    goto case 2;
                case 2:
                    *(short*)dest = *(short*)src;
                    return;
                case 1:
                    *dest = *src;
                    return;
                case 0:
                    return;
            }

            if (n <= 512)
            {
                int count = n / 32;
                n -= (n / 32) * 32;

                while (count > 0)
                {
                    ((long*)dest)[0] = ((long*)src)[0];
                    ((long*)dest)[1] = ((long*)src)[1];
                    ((long*)dest)[2] = ((long*)src)[2];
                    ((long*)dest)[3] = ((long*)src)[3];

                    dest += 32;
                    src += 32;
                    count--;
                }

                if (n > 16)
                {
                    ((long*)dest)[0] = ((long*)src)[0];
                    ((long*)dest)[1] = ((long*)src)[1];

                    src += 16;
                    dest += 16;
                    n -= 16;
                }

                goto SMALLTABLE;
            }

            BulkCopy(dest, src, n);
        }

        public unsafe static void Set(byte* dest, byte value, int n)
        {
            SetInline(dest, value, n);
        }

        /// <summary>
        /// Set is optimized to handle copy operations where n is statistically small.       
        /// </summary>
        /// <remarks>This is a forced inline version, use with care.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe static void SetInline(byte* dest, byte value, int n)
        {
            if (n == 0) 
                return;

            if (n < 512)
            {

                int block = 32, index = 0;
                int length = Math.Min(block, n);

                //Fill the initial array
                while (index < length)
                    dest[index++] = value;

                length = n;
                while (index < length)
                {
                    CopyInline(dest + index, dest, Math.Min(block, length - index));
                    index += block;
                    block *= 2;
                }
            }
            else UnmanagedMemory.Set(dest, value, n);
        }
    }
}
