using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Voron;

namespace Corax.Queries;

public enum StringDistanceAlgorithm
{
    None,
    NGram,
    JaroWinkler,
    Levenshtein
}

public struct LevenshteinDistance : IStringDistance
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public float GetDistance(Slice target, Slice other)
    {
        //Taken from RavenDB
        /*
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
        //TODO PERF
        var sa = target.ToString();
        var oth = other.ToString();
        int n = sa.Length;
        var buffer = QueryContext.MatchesPool.Rent(2 * (n + 1) * sizeof(int));
        var spanSize = sizeof(int) * (n + 1);
        var p = MemoryMarshal.Cast<byte, int>(buffer.AsSpan(0, spanSize));
        var d = MemoryMarshal.Cast<byte, int>(buffer.AsSpan(spanSize, spanSize));
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
        QueryContext.MatchesPool.Return(buffer);
        // our last action in the above loop was to switch d and p, so p now
        // actually has the most recent cost counts
        return result;
    }
}

public struct JaroWinklerDistance : IStringDistance
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public float GetDistance(Slice target, Slice other)
    {
        var s1 = target.ToString();
        var s2 = target.ToString();

        var mtp = Matches(s1, s2);
        var m = (float)mtp[0];

        if (Math.Abs(m - 0.0001) < 0)
            return 0f;

        float j = ((m / s1.Length + m / s2.Length + (m - mtp[1]) / m)) / 3;
        float jw = j < Constants.Suggestions.JaroWinklerThreshold ? j : j + Math.Min(0.1f, 1f / mtp[3]) * mtp[2] * (1 - j);
        return jw;
    }
    
    private static int[] Matches(string s1, string s2)
    {
        string max, min;

        if (s1.Length > s2.Length)
        {
            max = s1;
            min = s2;
        }
        else
        {
            max = s2;
            min = s1;
        }

        var range = Math.Max(max.Length / 2 - 1, 0);
        var matchIndexes = new int[min.Length];

        for (var i = 0; i < matchIndexes.Length; i++)
            matchIndexes[i] = -1;

        var matchFlags = new bool[max.Length];
        var matches = 0;

        for (var mi = 0; mi < min.Length; mi++)
        {
            var c1 = min[mi];
            for (int xi = Math.Max(mi - range, 0), xn = Math.Min(mi + range + 1, max.Length); xi < xn; xi++)
            {
                if (matchFlags[xi] || c1 != max[xi])
                    continue;

                matchIndexes[mi] = xi;
                matchFlags[xi] = true;
                matches++;
                break;
            }
        }

        var ms1 = new char[matches];
        var ms2 = new char[matches];

        for (int i = 0, si = 0; i < min.Length; i++)
        {
            if (matchIndexes[i] != -1)
            {
                ms1[si] = min[i];
                si++;
            }
        }

        for (int i = 0, si = 0; i < max.Length; i++)
        {
            if (matchFlags[i])
            {
                ms2[si] = max[i];
                si++;
            }
        }

        var transpositions = ms1.Where((t, mi) => t != ms2[mi]).Count();

        var prefix = 0;
        for (var mi = 0; mi < min.Length; mi++)
        {
            if (s1[mi] == s2[mi])
            {
                prefix++;
            }
            else
            {
                break;
            }
        }

        return new[] { matches, transpositions / 2, prefix, max.Length };
    }
}

public struct NoneStringDistance : IStringDistance
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public float GetDistance(Slice target, Slice other)
    {
        return 0f;
    }
}
