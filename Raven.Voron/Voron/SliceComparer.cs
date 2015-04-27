using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Voron.Util;

namespace Voron
{
    public sealed class SliceComparer : IEqualityComparer<Slice>, IComparer<Slice>
    {
        public static readonly SliceComparer Instance = new SliceComparer();
       
        public int Compare(Slice x, Slice y)
        {
            return CompareInline(x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(Slice x, Slice y)
        {
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            var srcKey = x.KeyLength;
            var otherKey = y.KeyLength;
            var size = srcKey <= otherKey ? srcKey : otherKey;

            int r = 0;

            unsafe
            {
                if (x.Array != null)
                {
                    fixed (byte* a = x.Array)
                    {
                        if (y.Array != null)
                        {
                            fixed (byte* b = y.Array)
                            {
                                r = MemoryUtils.CompareInline(a, b, size);
                            }
                        }
                        else
                        {
                            r = MemoryUtils.CompareInline(a, y.Pointer, size);
                        }
                    }
                }
                else
                {
                    if (y.Array != null)
                    {
                        fixed (byte* b = y.Array)
                        {
                            r = MemoryUtils.CompareInline(x.Pointer, b, size);
                        }
                    }
                    else
                    {
                        r = MemoryUtils.CompareInline(x.Pointer, y.Pointer, size);
                    }
                }
            }
            
            if (r != 0)
                return r;

            return srcKey - otherKey;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(Slice x, Slice y)
        {
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            var srcKey = x.KeyLength;
            var otherKey = y.KeyLength;
            if (srcKey != otherKey)
                return false;

            return CompareInline( x, y ) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(Slice obj)
        {
            return obj.GetHashCode();
        }
    }
}
