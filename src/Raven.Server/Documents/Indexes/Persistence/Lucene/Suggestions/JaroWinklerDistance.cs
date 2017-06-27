using System;
using System.Linq;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Suggestions
{
    internal sealed class JaroWinklerDistance : IStringDistance
    {
        public JaroWinklerDistance()
        {
            Threshold = 0.7f;
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

        public float GetDistance(string s1, string s2)
        {
            var mtp = Matches(s1, s2);
            var m = (float)mtp[0];

            if (Math.Abs(m - 0.0001) < 0)
                return 0f;

            float j = ((m / s1.Length + m / s2.Length + (m - mtp[1]) / m)) / 3;
            float jw = j < Threshold ? j : j + Math.Min(0.1f, 1f / mtp[3]) * mtp[2] * (1 - j);
            return jw;
        }

        /// <summary>
        /// Gets or sets the current value of the threshold used for adding the Winkler bonus.
        /// Set to a negative value to get the Jaro distance. The default value is 0.7.
        /// </summary>
        public float Threshold { get; set; }
    }
}