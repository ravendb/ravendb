using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Server.Strings
{
    public struct LevenshteinDistance : IStringDistance
    {
        public static readonly ArrayPool<int> MatchesPool = ArrayPool<int>.Create();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float GetDistance(ReadOnlySpan<byte> target, ReadOnlySpan<byte> other)
        {
            /*
                This implementation is based on Lucene.Net
                https://lucenenet.apache.org/docs/3.0.3/d9/d61/class_lucene_1_1_net_1_1_search_1_1_fuzzy_term_enum.html

               The difference between this impl. and the previous is that, rather
               than creating and retaining a matrix of size s.length()+1 by t.length()+1,
               we maintain two single-dimensional arrays of length s.length()+1.  The first, d,
               is the 'current working' distance array that maintains the newest distance cost
               counts as we iterate through the characters of string s.  Each time we increment
               the index of string t we are comparing, d is copied to p, the second int[].  Doing so
               allows us to retain the previous cost counts as required by the algorithm (taking
               the minimum of the cost count to the left, up one, and diagonally up and to the left
               of the current cost count being calculated).  (Note that the arrays aren't really
               copied anymore, just switched...this is clearly much better than cloning an array
               or doing a System.arraycopy() each time  through the outer loop.)

               Effectively, the difference between the two implementations is this one does not
               cause an out of memory condition when calculating the LD over two very large strings.
             */
           
            // TODO PERF: Check as there are a few high performance libraries to do this which include instruction level parallelism.           
            var sa = Encoding.UTF8.GetString(target.Slice(0, target.Length));
            var oth = Encoding.UTF8.GetString(other.Slice(0, other.Length - 1));
            int n = sa.Length;
            var buffer = MatchesPool.Rent(2 * (n + 1));
            var spanSize = n + 1;
            var p = buffer.AsSpan(0, spanSize);
            var d = buffer.AsSpan(spanSize, spanSize);
            int m = oth.Length;

            if (n == 0 || m == 0)
            {
                return n == m
                    ? 1
                    : 0;
            }

            // indexes into strings s and t
            int i; // iterates through s
            int j; // iterates through t

            for (i = 0; i <= n; i++)
            {
                p[i] = i;
            }

            for (j = 1; j <= m; j++)
            {
                var t_j = oth[j - 1]; // jth character of t
                d[0] = j;

                for (i = 1; i <= n; i++)
                {
                    var cost = sa[i - 1] == t_j ? 0 : 1;
                    // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
                    d[i] = Math.Min(Math.Min(d[i - 1] + 1, p[i] + 1), p[i - 1] + cost);
                }

                // copy current distance counts to 'previous row' distance counts
                var aux = p; //placeholder to assist in swapping p and d
                p = d;
                d = aux;
            }

            var result = 1.0f - ((float)p[n] / Math.Max(oth.Length, sa.Length));
            MatchesPool.Return(buffer);
            // our last action in the above loop was to switch d and p, so p now
            // actually has the most recent cost counts
            return result;
        }
    }
}
