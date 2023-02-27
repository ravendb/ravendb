using System;
using Sparrow;

namespace Voron.Data.CompactTrees;



unsafe partial class CompactTree
{
    // The random key scanner will take random paths down the tree to retrieve single keys at random.
    public struct RandomKeyScanner : IReadOnlySpanEnumerator
    {
        private readonly CompactTree _tree;
        private readonly int _samples;
        private readonly int _seed;
        private RandomIterator _iterator;

        public RandomKeyScanner(CompactTree tree, int samples, int? seed = null)
        {
            _tree = tree;
            _seed = seed.HasValue ? seed.Value : Random.Shared.Next();
            _samples = samples;
            _iterator = _tree.RandomIterate(samples, seed: _seed);
            _iterator.Reset();
        }

        public bool MoveNext(out ReadOnlySpan<byte> result)
        {
            return _iterator.MoveNext(out result, out long _);
        }

        public void Reset()
        {
            _iterator = _tree.RandomIterate(_samples, seed: _seed);
            _iterator.Reset();
        }
    }
}
