using System;
using Sparrow;

namespace Voron.Data.CompactTrees;



unsafe partial class CompactTree
{
    // The sequential key scanner would scan every single key available in the tree in sequential order.
    public struct SequentialKeyScanner : IReadOnlySpanEnumerator
    {
        private readonly CompactTree _tree;
        private Iterator _iterator;

        public SequentialKeyScanner(CompactTree tree)
        {
            _tree = tree;
            _iterator = _tree.Iterate();
            _iterator.Reset();
        }

        public bool MoveNext(out ReadOnlySpan<byte> result)
        {
            bool operationResult = _iterator.MoveNext(out Span<byte> resultSlice, out long _);
            result = resultSlice;
            return operationResult;
        }

        public void Reset()
        {
            _iterator = _tree.Iterate();
            _iterator.Reset();
        }
    }
}
