using System;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Suggestions
{
    internal sealed class NGramDistance : IStringDistance
    {
        private readonly int _n;

        /// <summary>
        /// Creates an N-Gram distance measure using n-grams of the specified size.
        /// </summary>
        /// <param name="size">The size of the n-gram to be used to compute the string distance.</param>
        public NGramDistance(int size)
        {
            _n = size;
        }

        /// <summary>
        /// Creates an N-Gram distance measure using n-grams of size 2.
        /// </summary>
        public NGramDistance()
            : this(2)
        {
        }

        public float GetDistance(string source, string target)
        {
            int sl = source.Length;
            int tl = target.Length;

            if (sl == 0 || tl == 0)
            {
                if (sl == tl)
                    return 1;

                return 0;
            }

            int cost = 0;
            if (sl < _n || tl < _n)
            {
                for (int ii = 0, ni = Math.Min(sl, tl); ii < ni; ii++)
                {
                    if (source[ii] == target[ii])
                    {
                        cost++;
                    }
                }
                return (float)cost / Math.Max(sl, tl);
            }

            //construct sa with prefix
            char[] sa = new char[sl + _n - 1];
            for (int ii = 0; ii < sa.Length; ii++)
            {
                if (ii < _n - 1)
                {
                    sa[ii] = (char)0; //add prefix
                }
                else
                {
                    sa[ii] = source[ii - _n + 1];
                }
            }

            var p = new float[sl + 1]; //'previous' cost array, horizontally
            var d = new float[sl + 1]; // cost array, horizontally

            // indexes into strings s and t
            int i; // iterates through source
            int j; // iterates through target

            char[] t_j = new char[_n]; // jth n-gram of t

            for (i = 0; i <= sl; i++)
            {
                p[i] = i;
            }

            for (j = 1; j <= tl; j++)
            {
                //construct t_j n-gram 
                if (j < _n)
                {
                    for (int ti = 0; ti < _n - j; ti++)
                    {
                        t_j[ti] = (char)0; //add prefix
                    }
                    for (int ti = _n - j; ti < _n; ti++)
                    {
                        t_j[ti] = target[ti - (_n - j)];
                    }
                }
                else
                {
                    t_j = target.Substring(j - _n, _n).ToCharArray();
                }
                d[0] = j;
                for (i = 1; i <= sl; i++)
                {
                    cost = 0;
                    int tn = _n;
                    //compare sa to t_j
                    for (int ni = 0; ni < _n; ni++)
                    {
                        if (sa[i - 1 + ni] != t_j[ni])
                        {
                            cost++;
                        }
                        else if (sa[i - 1 + ni] == 0)
                        { //discount matches on prefix
                            tn--;
                        }
                    }
                    float ec = (float)cost / tn;
                    // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
                    d[i] = Math.Min(Math.Min(d[i - 1] + 1, p[i] + 1), p[i - 1] + ec);
                }
                // copy current distance counts to 'previous row' distance counts
                var aux = p; //placeholder to assist in swapping p and d
                p = d;
                d = aux;
            }

            // our last action in the above loop was to switch d and p, so p now
            // actually has the most recent cost counts
            return 1.0f - ((float)p[sl] / Math.Max(tl, sl));
        }
    }
}