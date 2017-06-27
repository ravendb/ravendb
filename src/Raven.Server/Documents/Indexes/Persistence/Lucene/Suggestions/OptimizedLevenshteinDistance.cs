using System.Runtime.CompilerServices;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Suggestions
{
    /// <summary> Edit distance  class</summary>
    internal sealed class OptimizedLevenshteinDistance
    {
        private readonly char[] _sa;
        private readonly int _n;
        private readonly int[][][] _cache = new int[30][][];

        /// <summary> Optimized to run a bit faster than the static getDistance().
        /// In one benchmark times were 5.3sec using ctr vs 8.5sec w/ static method, thus 37% faster.
        /// </summary>
        public OptimizedLevenshteinDistance(string target)
        {
            _sa = target.ToCharArray();
            _n = _sa.Length;
        }


        //*****************************
        // Compute Levenshtein distance
        //*****************************
        public int GetDistance(string other)
        {
            int[][] d; // matrix

            // Step 1
            char[] ta = other.ToCharArray();
            int m = ta.Length;
            if (_n == 0)
            {
                return m;
            }
            if (m == 0)
            {
                return _n;
            }

            if (m >= _cache.Length)
            {
                d = Form(_n, m);
            }
            else if (_cache[m] != null)
            {
                d = _cache[m];
            }
            else
            {
                d = _cache[m] = Form(_n, m);

                // Step 3
            }
            for (int i = 1; i <= _n; i++)
            {
                char s_i = _sa[i - 1];

                // Step 4

                for (int j = 1; j <= m; j++)
                {
                    char t_j = ta[j - 1];

                    // Step 5

                    int cost = s_i == t_j ? 0 : 1;
                    d[i][j] = Min3(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + cost);
                }
            }

            // Step 7
            return d[_n][m];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int[][] Form(int n, int m)
        {
            int[][] d = new int[n + 1][];
            for (int i = 0; i < n + 1; i++)
            {
                d[i] = new int[m + 1];
            }
            // Step 2

            for (int i = 0; i <= n; i++)
            {
                d[i][0] = i;
            }
            for (int j = 0; j <= m; j++)
            {
                d[0][j] = j;
            }
            return d;
        }


        //****************************
        // Get minimum of three values
        //****************************
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Min3(int a, int b, int c)
        {
            int mi = a;
            if (b < mi)
            {
                mi = b;
            }
            if (c < mi)
            {
                mi = c;
            }
            return mi;
        }
    }
}