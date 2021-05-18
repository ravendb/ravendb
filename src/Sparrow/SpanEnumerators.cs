using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow
{
    public interface IReadOnlySpanEnumerator
    {
        int Length { get; }

        ReadOnlySpan<byte> this[int i] { get; }
    }

    public interface ISpanEnumerator
    {
        int Length { get; }

        Span<byte> this[int i] { get; }
    }
}
