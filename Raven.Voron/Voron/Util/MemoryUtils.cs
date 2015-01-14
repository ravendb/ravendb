using System.Runtime.CompilerServices;

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
                    case 1:
                        {
                            return lhs[0] - rhs[0];
                        }
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

        public static unsafe void Copy(byte* dest, byte* src, int n)
        {
            while (true) // Unrolling while with no pointers
            {
                switch (n)
                {
                    case 0: return;
                    case 1:
                        {
                            dest[0] = src[0];
                            return;
                        }
                    case 2:
                        {
                            dest[0] = src[0];
                            dest[1] = src[1];
                            return;
                        }
                    case 3:
                        {
                            dest[0] = src[0];
                            dest[1] = src[1];
                            dest[2] = src[2];
                            return;
                        }
                    default:
                        {
                            if (n <= sizeOfUlong * 2)
                            {
                                dest[0] = src[0];
                                dest[1] = src[1];
                                dest[2] = src[2];
                                dest[3] = src[3];

                                n -= 4;
                                src += 4;
                                dest += 4;
                            }
                            else 
                            {
                                ulong* srcPtr = (ulong*)src;
                                ulong* destPtr = (ulong*)dest;

                                while (n > sizeOfUlong * 8)
                                {
                                    destPtr[0] = srcPtr[0];
                                    destPtr[1] = srcPtr[1];
                                    destPtr[2] = srcPtr[2];
                                    destPtr[3] = srcPtr[3];

                                    destPtr[4] = srcPtr[4];
                                    destPtr[5] = srcPtr[5];
                                    destPtr[6] = srcPtr[6];
                                    destPtr[7] = srcPtr[7];

                                    srcPtr += 8;
                                    destPtr += 8;
                                    n -= sizeOfUlong * 8;
                                }

                                while (n > sizeOfUlong * 4)
                                {
                                    destPtr[0] = srcPtr[0];
                                    destPtr[1] = srcPtr[1];
                                    destPtr[2] = srcPtr[2];
                                    destPtr[3] = srcPtr[3];

                                    srcPtr += 4;
                                    destPtr += 4;
                                    n -= sizeOfUlong * 4;
                                }

                                while (n > sizeOfUlong * 2)
                                {
                                    destPtr[0] = srcPtr[0];
                                    destPtr[1] = srcPtr[1];

                                    srcPtr += 2;
                                    destPtr += 2;
                                    n -= sizeOfUlong * 2;
                                }

                                src = (byte*)srcPtr;
                                dest = (byte*)destPtr;
                            }
                            break;
                        }
                }
            }
        }
    }
}