using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Lucene.Net.Search
{
    static class Extensions
    {
        internal static bool EqualsToArrayList(this ArrayList me, ArrayList other)
        {
            if (me.Count != other.Count) return false;
            for (int i = 0; i < me.Count; i++)
            {
                if (me[i].Equals(other[i]) == false) return false;
            }
            return true;
        }
    }
}
