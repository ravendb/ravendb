using System;

namespace Sparrow.Server.Strings
{
    public interface IStringDistance
    {
        float GetDistance(ReadOnlySpan<byte> target, ReadOnlySpan<byte> other);
    }
}
