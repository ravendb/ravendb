using System;
using System.Collections.Generic;
using Voron.Trees;

namespace Voron.Impl
{
    internal class TreeAndSliceComparer : IEqualityComparer<Tuple<Tree, Slice>>
    {
        public bool Equals(Tuple<Tree, Slice> x, Tuple<Tree, Slice> y)
        {
            if (x == null && y == null)
                return true;
            if (x == null || y == null)
                return false;

            if (x.Item1 != y.Item1)
                return false;

            return x.Item2.Compare(y.Item2) == 0;
        }

        public int GetHashCode(Tuple<Tree, Slice> obj)
        {
            return obj.Item1.GetHashCode() ^ 397 * obj.Item2.GetHashCode();
        }
    }
}
