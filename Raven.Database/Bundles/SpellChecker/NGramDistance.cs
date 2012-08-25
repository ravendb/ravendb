/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


using System;
using System.Collections.Generic;
using System.Text;

namespace SpellChecker.Net.Search.Spell
{
    public class NGramDistance : StringDistance
    {
        private int n;

        /// <summary>
        /// Creates an N-Gram distance measure using n-grams of the specified size.
        /// </summary>
        /// <param name="size">The size of the n-gram to be used to compute the string distance.</param>
        public NGramDistance(int size)
        {
            this.n = size;
        }

        /// <summary>
        /// Creates an N-Gram distance measure using n-grams of size 2.
        /// </summary>
        public NGramDistance()
            : this(2)
        {
        }

        public float GetDistance(String source, String target)
        {
            int sl = source.Length;
            int tl = target.Length;

            if (sl == 0 || tl == 0)
            {
                if (sl == tl)
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
            }

            int cost = 0;
            if (sl < n || tl < n)
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

            char[] sa = new char[sl + n - 1];
            float[] p; //'previous' cost array, horizontally
            float[] d; // cost array, horizontally
            float[] _d; //placeholder to assist in swapping p and d

            //construct sa with prefix
            for (int ii = 0; ii < sa.Length; ii++)
            {
                if (ii < n - 1)
                {
                    sa[ii] = (char)0; //add prefix
                }
                else
                {
                    sa[ii] = source[ii - n + 1];
                }
            }
            p = new float[sl + 1];
            d = new float[sl + 1];

            // indexes into strings s and t
            int i; // iterates through source
            int j; // iterates through target

            char[] t_j = new char[n]; // jth n-gram of t

            for (i = 0; i <= sl; i++)
            {
                p[i] = i;
            }

            for (j = 1; j <= tl; j++)
            {
                //construct t_j n-gram 
                if (j < n)
                {
                    for (int ti = 0; ti < n - j; ti++)
                    {
                        t_j[ti] = (char)0; //add prefix
                    }
                    for (int ti = n - j; ti < n; ti++)
                    {
                        t_j[ti] = target[ti - (n - j)];
                    }
                }
                else
                {
                    t_j = target.Substring(j - n, n).ToCharArray();
                }
                d[0] = j;
                for (i = 1; i <= sl; i++)
                {
                    cost = 0;
                    int tn = n;
                    //compare sa to t_j
                    for (int ni = 0; ni < n; ni++)
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
                _d = p;
                p = d;
                d = _d;
            }

            // our last action in the above loop was to switch d and p, so p now
            // actually has the most recent cost counts
            return 1.0f - ((float)p[sl] / Math.Max(tl, sl));
        }


    }
}
