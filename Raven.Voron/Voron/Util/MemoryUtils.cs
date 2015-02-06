using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Voron.Impl;

namespace Voron.Util
{
    public unsafe static class MemoryUtils
    {
        public static SliceComparer MemoryComparerInstance = Compare;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Compare(byte* bpx, byte* bpy, int n)
        {
            switch (n)
            {
                case 0: return 0;
                case 1: return *bpx - *bpy;
                case 2:
                    {
                        int v = *bpx - *bpy;
                        if (v != 0)
                            return v;

                        bpx++;
                        bpy++;

                        return *bpx - *bpy;
                    }
             
                default: return StdLib.memcmp(bpx, bpy, n);
            }
        }

        /// <summary>
        /// Bulk copy is optimized to handle copy operations where n is statistically big. While it will use a faster copy operation for 
        /// small amounts of memory, when you have smaller than 2048 bytes calls (depending on the target CPU) it will always be
        /// faster to call .Copy() directly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe static void BulkCopy(byte* dest, byte* src, int n)
        {
            StdLib.memcpy(dest, src, n);            
        }

        /// <summary>
        /// Copy is optimized to handle copy operations where n is statistically small. 
        /// This method is optimized at the IL level to be extremely efficient for copies smaller than
        /// 4096 bytes or heterogeneous workloads with occasional big copies.         
        /// </summary>
        public unsafe static void Copy(byte* dest, byte* src, int n)
        {
            SMALLTABLE:
            switch (n)
            {
                case 0:
                    return;
                case 1:
                    *dest = *src;
                    return;
                case 2:
                    *(short*)dest = *(short*)src;
                    return;
                case 3:
                    *(dest + 2) = *(src + 2);
                    goto case 2;
                case 4:
                    *(int*)dest = *(int*)src;
                    return;
                case 5:
                    *(dest + 4) = *(src + 4);
                    goto case 4;
                case 6:
                    *(short*)(dest + 4) = *(short*)(src + 4);
                    goto case 4;
                case 7:
                    *(short*)(dest + 4) = *(short*)(src + 4);
                    *(dest + 6) = *(src + 6);
                    goto case 4;
                case 8:
                    *(long*)dest = *(long*)src;
                    return;
                case 9:
                    *(dest + 8) = *(src + 8);
                    goto case 8;
                case 10:
                    *(short*)(dest + 8) = *(short*)(src + 8);
                    goto case 8;
                case 11:
                    *(short*)(dest + 8) = *(short*)(src + 8);
                    *(dest + 10) = *(src + 10);
                    goto case 8;
                case 12:
                    *(long*)dest = *(long*)src;
                    *(int*)(dest + 8) = *(int*)(src + 8);
                    return;
                case 13:                    
                    *(dest + 12) = *(src + 12);
                    goto case 12;
                case 14:
                    *(short*)(dest + 12) = *(short*)(src + 12);
                    goto case 12;
                case 15:
                    *(short*)(dest + 12) = *(short*)(src + 12);
                    *(dest + 14) = *(src + 14);
                    goto case 12;
                case 16:
                    *(long*)dest = *(long*)src;
                    *(long*)(dest + 8) = *(long*)(src + 8);
                    return;
                default:
                    break;
            }

            if (n <= 128)
            {
                int count = n / 32;
                n -= count * 32;

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

            StdLib.memcpy(dest, src, n);
        }
    }
}