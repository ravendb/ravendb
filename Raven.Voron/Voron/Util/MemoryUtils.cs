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

        private const int sizeOfUlong = sizeof(ulong);
        private const int sizeOfUlongThreshold = sizeOfUlong * 4;

        public static int Compare(byte* lhs, byte* rhs, int n)
        {
            // Fast switch (20% from original)
            switch (n)
            {
                case 0: return 0;
                case 1: return lhs[0] - rhs[0];
                case 2:
                    {
                        var v = lhs[0] - rhs[0];
                        if (v != 0)
                            return v;

                        return lhs[1] - rhs[1];
                    }
                case 3:
                    {
                        var v = lhs[0] - rhs[0];
                        if (v != 0)
                            return v;

                        v = lhs[1] - rhs[1];
                        if (v != 0)
                            return v;

                        return lhs[2] - rhs[2];
                    }
                default:
                    {
                        var v = lhs[0] - rhs[0];
                        if (v != 0)
                            return v;

                        v = lhs[1] - rhs[1];
                        if (v != 0)
                            return v;

                        v = lhs[2] - rhs[2];
                        if (v != 0)
                            return v;

                        v = lhs[3] - rhs[3];
                        if (v != 0)
                            return v;

                        n -= 4;
                        lhs += 4;
                        rhs += 4;
                        break;
                    }
            }

            if (n >= sizeOfUlongThreshold)
            {
                var lUintAlignment = (long)lhs % sizeOfUlong;
                var rUintAlignment = (long)rhs % sizeOfUlong;

                if (lUintAlignment != 0 && lUintAlignment == rUintAlignment)
                {
                    var toAlign = sizeOfUlong - lUintAlignment;
                    while (toAlign > 0)
                    {
                        var r = lhs[0] - rhs[0]; // No pointers access
                        if (r != 0)
                            return r;

                        lhs++;
                        rhs++;
                        n--;

                        toAlign--;
                    }
                }

                // Higher bandwidth will improve more with longer memory compares (20% with 32bytes, 50% with 256)
                ulong* lp = (ulong*)lhs;
                ulong* rp = (ulong*)rhs;

                while (n > sizeOfUlong) // No pointers improvement
                {
                    if (lp[0] != rp[0])
                        break;

                    lp += 1;
                    rp += 1;
                    n -= sizeOfUlong;
                }

                lhs = (byte*)lp;
                rhs = (byte*)rp;
            }

            while (true) // Unrolling while with no pointers
            {
                switch (n)
                {
                    case 0: return 0;
                    case 1: return lhs[0] - rhs[0];
                    case 2:
                        {
                            var v = lhs[0] - rhs[0];
                            if (v != 0)
                                return v;

                            return lhs[1] - rhs[1];
                        }
                    case 3:
                        {
                            var v = lhs[0] - rhs[0];
                            if (v != 0)
                                return v;

                            v = lhs[1] - rhs[1];
                            if (v != 0)
                                return v;

                            return lhs[2] - rhs[2];
                        }
                    default:
                        {
                            var v = lhs[0] - rhs[0];
                            if (v != 0)
                                return v;

                            v = lhs[1] - rhs[1];
                            if (v != 0)
                                return v;

                            v = lhs[2] - rhs[2];
                            if (v != 0)
                                return v;

                            v = lhs[3] - rhs[3];
                            if (v != 0)
                                return v;

                            n -= 4;
                            lhs += 4;
                            rhs += 4;
                            break;
                        }
                }
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
            if (n < 1024 * 1024 * 4)
            {
                StdLib.memcpy(dest, src, n);
            }
            else
            {
                InnerBulkCopyMT(dest, src, n);
            }               
        }

        public unsafe static void Memcpy(byte* dest, byte* src, int len)
        {
            //
            // This is portable version of memcpy. It mirrors what the hand optimized assembly versions of memcpy typically do.
            //
            // Ideally, we would just use the cpblk IL instruction here. Unfortunately, cpblk IL instruction is not as efficient as
            // possible yet and so we have this implementation here for now.
            //

            switch (len)
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
                    *(short*)dest = *(short*)src;
                    *(dest + 2) = *(src + 2);
                    return;
                case 4:
                    *(int*)dest = *(int*)src;
                    return;
                case 5:
                    *(int*)dest = *(int*)src;
                    *(dest + 4) = *(src + 4);
                    return;
                case 6:
                    *(int*)dest = *(int*)src;
                    *(short*)(dest + 4) = *(short*)(src + 4);
                    return;
                case 7:
                    *(int*)dest = *(int*)src;
                    *(short*)(dest + 4) = *(short*)(src + 4);
                    *(dest + 6) = *(src + 6);
                    return;
                case 8:
                    *(long*)dest = *(long*)src;
                    return;
                case 9:
                    *(long*)dest = *(long*)src;
                    *(dest + 8) = *(src + 8);
                    return;
                case 10:
                    *(long*)dest = *(long*)src;
                    *(short*)(dest + 8) = *(short*)(src + 8);
                    return;
                case 11:
                    *(long*)dest = *(long*)src;
                    *(short*)(dest + 8) = *(short*)(src + 8);
                    *(dest + 10) = *(src + 10);
                    return;
                case 12:
                    *(long*)dest = *(long*)src;
                    *(int*)(dest + 8) = *(int*)(src + 8);
                    return;
                case 13:
                    *(long*)dest = *(long*)src;
                    *(int*)(dest + 8) = *(int*)(src + 8);
                    *(dest + 12) = *(src + 12);
                    return;
                case 14:
                    *(long*)dest = *(long*)src;
                    *(int*)(dest + 8) = *(int*)(src + 8);
                    *(short*)(dest + 12) = *(short*)(src + 12);
                    return;
                case 15:
                    *(long*)dest = *(long*)src;
                    *(int*)(dest + 8) = *(int*)(src + 8);
                    *(short*)(dest + 12) = *(short*)(src + 12);
                    *(dest + 14) = *(src + 14);
                    return;
                case 16:
                    *(long*)dest = *(long*)src;
                    *(long*)(dest + 8) = *(long*)(src + 8);
                    return;
                default:
                    break;
            }

            // P/Invoke into the native version for large lengths
            if (len >= 512)
            {
                StdLib.memcpy(dest, src, len);
                return;
            }

            if (((int)dest & 3) != 0)
            {
                if (((int)dest & 1) != 0)
                {
                    *dest = *src;
                    src++;
                    dest++;
                    len--;
                    if (((int)dest & 2) == 0)
                        goto Aligned;
                }
                *(short*)dest = *(short*)src;
                src += 2;
                dest += 2;
                len -= 2;
            Aligned: ;
            }

            if (((int)dest & 4) != 0)
            {
                *(int*)dest = *(int*)src;
                src += 4;
                dest += 4;
                len -= 4;
            }

            int count = len / 16;
            while (count > 0)
            {
                ((long*)dest)[0] = ((long*)src)[0];
                ((long*)dest)[1] = ((long*)src)[1];

                dest += 16;
                src += 16;
                count--;
            }

            if ((len & 8) != 0)
            {
                ((long*)dest)[0] = ((long*)src)[0];
                dest += 8;
                src += 8;
            }

            if ((len & 4) != 0)
            {
                ((int*)dest)[0] = ((int*)src)[0];
                dest += 4;
                src += 4;
            }

            if ((len & 2) != 0)
            {
                ((short*)dest)[0] = ((short*)src)[0];
                dest += 2;
                src += 2;
            }

            if ((len & 1) != 0)
                *dest++ = *src++;
        }

        private unsafe static void InnerBulkCopyMT(byte* dest, byte* src, int n)
        {
            var threadcount = Math.Min(3, Environment.ProcessorCount);
            var chunksize = n / threadcount;
            var remainder = n % threadcount;

            var tasks = new Action[threadcount];
            for (var i = 0; i < threadcount - 1; ++i)
            {
                var offset = i * chunksize;
                var newSrc = src + offset;
                var newDst = dest + offset;
                tasks[i] = () => StdLib.memcpy(newSrc, newDst, chunksize);
            }

            var finalOffset = (threadcount - 1) * chunksize;
            var finalSrc = src + finalOffset;
            var finalDst = dest + finalOffset;
            tasks[threadcount - 1] = () => StdLib.memcpy(finalSrc, finalDst, remainder);

            Parallel.Invoke(tasks);
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

            if (n <= 2048)
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

            if ( n < 1024 * 1024 * 4)
            {
                StdLib.memcpy(dest, src, n);
            }
            else
            {
                InnerBulkCopyMT(dest, src, n);
            }
        }

    }
}