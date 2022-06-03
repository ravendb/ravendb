using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Server.Strings
{
    public struct JaroWinklerDistance : IStringDistance
    {
        private readonly float _threshold;

        public const float Threshold = 0.5f;

        public JaroWinklerDistance(float threshold = Threshold)
        {
            _threshold = threshold;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float GetDistance(ReadOnlySpan<byte> target, ReadOnlySpan<byte> other)
        {
            var s1 = Encoding.UTF8.GetString(target.Slice(0, target.Length));
            var s2 = Encoding.UTF8.GetString(other.Slice(0, other.Length - 1));

            var mtp = Matches(s1, s2);
            var m = (float)mtp[0];

            if (Math.Abs(m - 0.0001) < 0)
                return 0f;

            float j = ((m / s1.Length + m / s2.Length + (m - mtp[1]) / m)) / 3;
            float jw = j < _threshold ? j : j + Math.Min(0.1f, 1f / mtp[3]) * mtp[2] * (1 - j);
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
}
