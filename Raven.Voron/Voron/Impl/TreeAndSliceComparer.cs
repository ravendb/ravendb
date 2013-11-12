using System;
using System.Collections.Generic;
using Voron.Trees;

namespace Voron.Impl
{
    internal unsafe class TreeAndSliceComparer : IEqualityComparer<Tuple<Tree, Slice>>
    {
        private readonly SliceComparer _comparer;

        public TreeAndSliceComparer(SliceComparer comparer)
        {
            _comparer = comparer;
        }

        public bool Equals(Tuple<Tree, Slice> x, Tuple<Tree, Slice> y)
        {
            if (x == null && y == null)
                return true;
            if (x == null || y == null)
                return false;

            if (x.Item1 != y.Item1)
                return false;

            return x.Item2.Compare(y.Item2, _comparer) == 0;
        }

        public int GetHashCode(Tuple<Tree, Slice> obj)
        {
            return obj.Item1.GetHashCode() ^ 397 * obj.Item2.GetHashCode();
        }
    }
}