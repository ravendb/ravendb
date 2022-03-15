using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron
{
    public static class SliceExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]        
        public static bool StartWith(this Slice s1, ReadOnlySpan<byte> s2)
        {
            return s1.AsReadOnlySpan().StartsWith(s2);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]        
        public static bool EndsWith(this Slice s1, ReadOnlySpan<byte> s2)
        {
            return s1.AsReadOnlySpan().EndsWith(s2);
        }
    }
}
