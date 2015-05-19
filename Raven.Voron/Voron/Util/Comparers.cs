using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Util
{
    internal class LongEqualityComparer : IEqualityComparer<long>
    {
        public static readonly LongEqualityComparer Instance = new LongEqualityComparer();

        public bool Equals(long x, long y)
        {
            return x == y;
        }

        public int GetHashCode(long obj)
        {
            return unchecked((int)obj ^ (int)(obj >> 32));
        }
    }

    internal class IntEqualityComparer : IEqualityComparer<int>
    {
        public static readonly IntEqualityComparer Instance = new IntEqualityComparer();

        public bool Equals(int x, int y)
        {
            return x == y;
        }

        public int GetHashCode(int obj)
        {
            return obj;
        }
    }

}
