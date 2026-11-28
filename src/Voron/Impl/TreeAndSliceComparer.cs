using System;
using System.Collections.Generic;
using Voron.Data.BTrees;

namespace Voron.Impl
{
    internal sealed class TreeAndSliceComparer : IEqualityComparer<(Tree Parent, Slice Key)>
    {
        public static readonly TreeAndSliceComparer Instance = new();

        public bool Equals((Tree Parent, Slice Key) x, (Tree Parent, Slice Key) y)
        {
            return x.Parent == y.Parent && SliceComparer.Equals(x.Key, y.Key);
        }

        public int GetHashCode((Tree Parent, Slice Key) obj)
        {
            return obj.Parent.GetHashCode() ^ 397 * (int)obj.Key.Content.GetContentHash();
        }
    }
}
