using System;
using System.Runtime.CompilerServices;

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
