using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow
{
    public interface IReadOnlySpanEnumerator
    {
        void Reset();
        bool MoveNext(out ReadOnlySpan<byte> result);
    }

    public interface IReadOnlySpanIndexer
    {
        int Length { get; }

        bool IsNull(int i);

        ReadOnlySpan<byte> this[int i] { get; }
    }

    public interface ISpanEnumerator
    {
        void Reset();
        bool MoveNext(out Span<byte> result);
    }

    public interface ISpanIndexer
    {
        int Length { get; }

        bool IsNull(int i);

        Span<byte> this[int i] { get; }
    }
}
