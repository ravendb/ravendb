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

using System;
using Lucene.Net.Store;
using IndexReader = Lucene.Net.Index.IndexReader;
using Term = Lucene.Net.Index.Term;

namespace Lucene.Net.Search
{
	
	/// <summary>Subclass of FilteredTermEnum for enumerating all terms that are similiar
	/// to the specified filter term.
	/// 
	/// <p/>Term enumerations are always ordered by Term.compareTo().  Each term in
	/// the enumeration is greater than all that precede it.
	/// </summary>
	public sealed class FuzzyTermEnum:FilteredTermEnum
	{
		/* Allows us save time required to create a new array
		* everytime similarity is called.
		*/
	    private int[] p;
		private int[] d;
		
		private float similarity;
		private bool endEnum = false;

	    private bool isDisposed;

        private Term searchTerm = null;
		private System.String field;
		private System.String text;
		private System.String prefix;
		
		private float minimumSimilarity;
		private float scale_factor;
		
		/// <summary> Creates a FuzzyTermEnum with an empty prefix and a minSimilarity of 0.5f.
		/// <p/>
		/// After calling the constructor the enumeration is already pointing to the first 
		/// valid term if such a term exists. 
		/// 
		/// </summary>
		/// <param name="reader">
		/// </param>
		/// <param name="term">
		/// </param>
		/// <throws>  IOException </throws>
		/// <seealso cref="FuzzyTermEnum(IndexReader, Term, float, int)">
		/// </seealso>
		public FuzzyTermEnum(IndexReader reader, Term term, IState state) :this(reader, term, FuzzyQuery.defaultMinSimilarity, FuzzyQuery.defaultPrefixLength, state)
		{
		}
		
		/// <summary> Creates a FuzzyTermEnum with an empty prefix.
		/// <p/>
		/// After calling the constructor the enumeration is already pointing to the first 
		/// valid term if such a term exists. 
		/// 
		/// </summary>
		/// <param name="reader">
		/// </param>
		/// <param name="term">
		/// </param>
		/// <param name="minSimilarity">
		/// </param>
		/// <throws>  IOException </throws>
		/// <seealso cref="FuzzyTermEnum(IndexReader, Term, float, int)">
		/// </seealso>
		public FuzzyTermEnum(IndexReader reader, Term term, float minSimilarity, IState state) :this(reader, term, minSimilarity, FuzzyQuery.defaultPrefixLength, state)
		{
		}
		
		/// <summary> Constructor for enumeration of all terms from specified <c>reader</c> which share a prefix of
		/// length <c>prefixLength</c> with <c>term</c> and which have a fuzzy similarity &gt;
		/// <c>minSimilarity</c>.
		/// <p/>
		/// After calling the constructor the enumeration is already pointing to the first 
		/// valid term if such a term exists. 
		/// 
		/// </summary>
		/// <param name="reader">Delivers terms.
		/// </param>
		/// <param name="term">Pattern term.
		/// </param>
		/// <param name="minSimilarity">Minimum required similarity for terms from the reader. Default value is 0.5f.
		/// </param>
		/// <param name="prefixLength">Length of required common prefix. Default value is 0.
		/// </param>
		/// <throws>  IOException </throws>
		public FuzzyTermEnum(IndexReader reader, Term term, float minSimilarity, int prefixLength, IState state) :base()
		{
			
			if (minSimilarity >= 1.0f)
				throw new System.ArgumentException("minimumSimilarity cannot be greater than or equal to 1");
			else if (minSimilarity < 0.0f)
				throw new System.ArgumentException("minimumSimilarity cannot be less than 0");
			if (prefixLength < 0)
				throw new System.ArgumentException("prefixLength cannot be less than 0");
			
			this.minimumSimilarity = minSimilarity;
			this.scale_factor = 1.0f / (1.0f - minimumSimilarity);
			this.searchTerm = term;
			this.field = searchTerm.Field;
			
			//The prefix could be longer than the word.
			//It's kind of silly though.  It means we must match the entire word.
			int fullSearchTermLength = searchTerm.Text.Length;
			int realPrefixLength = prefixLength > fullSearchTermLength?fullSearchTermLength:prefixLength;
			
			this.text = searchTerm.Text.Substring(realPrefixLength);
			this.prefix = searchTerm.Text.Substring(0, (realPrefixLength) - (0));

		    this.p = new int[this.text.Length + 1];
            this.d = new int[this.text.Length + 1];
			
			SetEnum(reader.Terms(new Term(searchTerm.Field, prefix), state), state);
		}
		
		/// <summary> The termCompare method in FuzzyTermEnum uses Levenshtein distance to 
		/// calculate the distance between the given term and the comparing term. 
		/// </summary>
		protected internal override bool TermCompare(Term term)
		{
			if ((System.Object) field == (System.Object) term.Field && term.Text.StartsWith(prefix))
			{
				System.String target = term.Text.Substring(prefix.Length);
				this.similarity = Similarity(target);
				return (similarity > minimumSimilarity);
			}
			endEnum = true;
			return false;
		}
		
		public override float Difference()
		{
			return ((similarity - minimumSimilarity) * scale_factor);
		}
		
		public override bool EndEnum()
		{
			return endEnum;
		}
		
		// <summary>
		// ***************************
		// Compute Levenshtein distance
		// ****************************
		// </summary>
		
		/// <summary> <p/>Similarity returns a number that is 1.0f or less (including negative numbers)
		/// based on how similar the Term is compared to a target term.  It returns
		/// exactly 0.0f when
		/// <c>
		/// editDistance &gt; maximumEditDistance</c>  
		/// Otherwise it returns:
		/// <c>
		/// 1 - (editDistance / length)</c>
		/// where length is the length of the shortest term (text or target) including a
		/// prefix that are identical and editDistance is the Levenshtein distance for
		/// the two words.<p/>
		/// 
		/// <p/>Embedded within this algorithm is a fail-fast Levenshtein distance
		/// algorithm.  The fail-fast algorithm differs from the standard Levenshtein
		/// distance algorithm in that it is aborted if it is discovered that the
		/// mimimum distance between the words is greater than some threshold.
		/// 
		/// <p/>To calculate the maximum distance threshold we use the following formula:
		/// <c>
		/// (1 - minimumSimilarity) * length</c>
		/// where length is the shortest term including any prefix that is not part of the
		/// similarity comparision.  This formula was derived by solving for what maximum value
		/// of distance returns false for the following statements:
		/// <code>
		/// similarity = 1 - ((float)distance / (float) (prefixLength + Math.min(textlen, targetlen)));
        /// return (similarity > minimumSimilarity);</code>
		/// where distance is the Levenshtein distance for the two words.
		/// <p/>
		/// <p/>Levenshtein distance (also known as edit distance) is a measure of similiarity
		/// between two strings where the distance is measured as the number of character
		/// deletions, insertions or substitutions required to transform one string to
		/// the other string.
		/// </summary>
		/// <param name="target">the target word or phrase
		/// </param>
		/// <returns> the similarity,  0.0 or less indicates that it matches less than the required
		/// threshold and 1.0 indicates that the text and target are identical
		/// </returns>
        private float Similarity(System.String target)
        {

            int m = target.Length;
            int n = text.Length;
            if (n == 0)
            {
                //we don't have anything to compare.  That means if we just add
                //the letters for m we get the new word
                return prefix.Length == 0 ? 0.0f : 1.0f - ((float)m / prefix.Length);
            }
            if (m == 0)
            {
                return prefix.Length == 0 ? 0.0f : 1.0f - ((float)n / prefix.Length);
            }

            int maxDistance = CalculateMaxDistance(m);

            if (maxDistance < System.Math.Abs(m - n))
            {
                //just adding the characters of m to n or vice-versa results in
                //too many edits
                //for example "pre" length is 3 and "prefixes" length is 8.  We can see that
                //given this optimal circumstance, the edit distance cannot be less than 5.
                //which is 8-3 or more precisesly Math.abs(3-8).
                //if our maximum edit distance is 4, then we can discard this word
                //without looking at it.
                return 0.0f;
            }

            // init matrix d
            for (int i = 0; i < n; ++i)
            {
                p[i] = i;
            }

                // start computing edit distance
            for (int j = 1; j <= m; ++j)
            {
                int bestPossibleEditDistance = m;
                char t_j = target[j - 1];
                d[0] = j;
                for (int i = 1; i <= n; ++i)
                {
                    // minimum of cell to the left+1, to the top+1, diagonally left and up +(0|1)
                    if (t_j != text[i - 1])
                    {
                        d[i] = Math.Min(Math.Min(d[i - 1], p[i]), p[i - 1]) + 1;
                    }
                    else
                    {
                        d[i] = Math.Min(Math.Min(d[i - 1] + 1, p[i] + 1), p[i - 1]);
                    }
                    bestPossibleEditDistance = System.Math.Min(bestPossibleEditDistance, d[i]);
                }

                //After calculating row i, the best possible edit distance
                //can be found by found by finding the smallest value in a given column.
                //If the bestPossibleEditDistance is greater than the max distance, abort.

                if (j > maxDistance && bestPossibleEditDistance > maxDistance)
                {
                    //equal is okay, but not greater
                    //the closest the target can be to the text is just too far away.
                    //this target is leaving the party early.
                    return 0.0f;
                }

                // copy current distance counts to 'previous row' distance counts: swap p and d
                  int[] _d = p;
                  p = d;
                  d = _d;
            }

            // our last action in the above loop was to switch d and p, so p now
            // actually has the most recent cost counts

            // this will return less than 0.0 when the edit distance is
            // greater than the number of characters in the shorter word.
            // but this was the formula that was previously used in FuzzyTermEnum,
            // so it has not been changed (even though minimumSimilarity must be
            // greater than 0.0)
            return 1.0f - (p[n] / (float)(prefix.Length + System.Math.Min(n, m)));

        }
		
		/// <summary> The max Distance is the maximum Levenshtein distance for the text
		/// compared to some other value that results in score that is
		/// better than the minimum similarity.
		/// </summary>
		/// <param name="m">the length of the "other value"
		/// </param>
		/// <returns> the maximum levenshtein distance that we care about
		/// </returns>
		private int CalculateMaxDistance(int m)
		{
			return (int) ((1 - minimumSimilarity) * (System.Math.Min(text.Length, m) + prefix.Length));
		}

		protected override void Dispose(bool disposing)
		{
            if (isDisposed) return;

            if (disposing)
            {
                p = null;
                d = null;
                searchTerm = null;
            }

		    isDisposed = true;
            base.Dispose(disposing); //call super.close() and let the garbage collector do its work.
		}
	}
}