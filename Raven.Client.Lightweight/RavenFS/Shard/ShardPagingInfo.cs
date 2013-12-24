using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Client.RavenFS.Shard
{
    public class PagingInfo
    {
        private readonly Dictionary<int, int[]> positions = new Dictionary<int, int[]>();

        public PagingInfo(int size)
        {
            positions.Add(0, new int[size]);
        }

        public void SetPagingInfo(int[] offsets)
        {
            positions[CurrentPage + 1] = offsets; // current page results is the offset for the next page
        }

        public int CurrentPage { get; set; }

        public int[] GetPagingInfo(int page)
        {
            int[] ints;
            if (positions.TryGetValue(page, out ints) == false)
                return null;

            var clone = new int[ints.Length];
            Buffer.BlockCopy(ints, 0, clone, 0, ints.Length * sizeof(int));
            return clone;
        }

        public int GetLastPageNumber()
        {
            return positions.Keys.Max();
        }
    }
}
