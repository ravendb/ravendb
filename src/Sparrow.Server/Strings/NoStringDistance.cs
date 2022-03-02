using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Server.Strings
{
    public struct NoStringDistance : IStringDistance
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float GetDistance(ReadOnlySpan<byte> target, ReadOnlySpan<byte> other)
        {
            return 0f;
        }
    }
}
