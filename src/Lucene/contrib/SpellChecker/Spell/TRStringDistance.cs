/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


namespace SpellChecker.Net.Search.Spell
{
	
    /// <summary> Edit distance  class</summary>
    public class TRStringDistance
    {
		
        internal char[] sa;
        internal int n;
        internal int[][][] cache = new int[30][][];
		
		
        /// <summary> Optimized to run a bit faster than the static getDistance().
        /// In one benchmark times were 5.3sec using ctr vs 8.5sec w/ static method, thus 37% faster.
        /// </summary>
        public TRStringDistance(System.String target)
        {
            sa = target.ToCharArray();
            n = sa.Length;
        }
		
		
        //***************************
        // Compute Levenshtein distance
        //***************************
        public int GetDistance(System.String other)
        {
            int[][] d; // matrix

            // Step 1
            char[] ta = other.ToCharArray();
            int m = ta.Length;
            if (n == 0)
            {
                return m;
            }
            if (m == 0)
            {
                return n;
            }
			
            if (m >= cache.Length)
            {
                d = Form(n, m);
            }
            else if (cache[m] != null)
            {
                d = cache[m];
            }
            else
            {
                d = cache[m] = Form(n, m);
				
                // Step 3
            }
            for (int i = 1; i <= n; i++)
            {
                char s_i = sa[i - 1];
				
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
            return d[n][m];
        }
		
		
        /// <summary> </summary>
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
		
		
        //**************************
        // Get minimum of three values
        //**************************
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