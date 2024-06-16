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
using System.Collections.Generic;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Util;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using Version = Lucene.Net.Util.Version;

namespace Lucene.Net.Analysis
{
	
	/// <summary> Removes stop words from a token stream.</summary>
	
	public sealed class StopFilter:TokenFilter
	{
		private readonly CharArraySet stopWords;
		private bool enablePositionIncrements = false;
		
		private readonly ITermAttribute termAtt;
		private readonly IPositionIncrementAttribute posIncrAtt;
		
		/// <summary> Construct a token stream filtering the given input.
		/// If <c>stopWords</c> is an instance of <see cref="CharArraySet" /> (true if
		/// <c>makeStopSet()</c> was used to construct the set) it will be directly used
		/// and <c>ignoreCase</c> will be ignored since <c>CharArraySet</c>
		/// directly controls case sensitivity.
		/// <p/>
		/// If <c>stopWords</c> is not an instance of <see cref="CharArraySet" />,
		/// a new CharArraySet will be constructed and <c>ignoreCase</c> will be
		/// used to specify the case sensitivity of that set.
		/// </summary>
		/// <param name="enablePositionIncrements">true if token positions should record the removed stop words</param>
		/// <param name="input">Input TokenStream</param>
		/// <param name="stopWords">A Set of strings or strings or char[] or any other ToString()-able set representing the stopwords</param>
        /// <param name="ignoreCase">if true, all words are lower cased first</param>
        public StopFilter(bool enablePositionIncrements, TokenStream input, ISet<string> stopWords, bool ignoreCase)
            : base(input)
		{
		    if (stopWords is CharArraySet)
		    {
		        this.stopWords = (CharArraySet) stopWords;
		    }
		    else
		    {
		        this.stopWords = new CharArraySet(stopWords.Count, ignoreCase);
		        this.stopWords.AddAll(stopWords);
		    }
		    this.enablePositionIncrements = enablePositionIncrements;
		    termAtt = AddAttribute<ITermAttribute>();
            posIncrAtt = AddAttribute<IPositionIncrementAttribute>();
		}

	    /// <summary> Constructs a filter which removes words from the input
		/// TokenStream that are named in the Set.
		/// </summary>
		/// <param name="enablePositionIncrements">true if token positions should record the removed stop words</param>
		///  <param name="in">Input stream</param>
		/// <param name="stopWords">A Set of strings or char[] or any other ToString()-able set representing the stopwords</param>
		/// <seealso cref="MakeStopSet(String[])"/>
		public StopFilter(bool enablePositionIncrements, TokenStream @in, ISet<string> stopWords)
			: this(enablePositionIncrements, @in, stopWords, false)
		{ }
		
		/// <summary> Builds a Set from an array of stop words,
		/// appropriate for passing into the StopFilter constructor.
		/// This permits this stopWords construction to be cached once when
		/// an Analyzer is constructed.
		/// 
		/// </summary>
		/// <seealso cref="MakeStopSet(String[], bool)">passing false to ignoreCase</seealso>
		public static ISet<string> MakeStopSet(params string[] stopWords)
		{
			return MakeStopSet(stopWords, false);
		}
		
		/// <summary> Builds a Set from an array of stop words,
		/// appropriate for passing into the StopFilter constructor.
		/// This permits this stopWords construction to be cached once when
		/// an Analyzer is constructed.
		/// </summary>
		/// <param name="stopWords">A list of strings or char[] or any other ToString()-able list representing the stop words</param>
		/// <seealso cref="MakeStopSet(String[], bool)">passing false to ignoreCase</seealso>
		public static ISet<string> MakeStopSet(IList<object> stopWords)
		{
			return MakeStopSet(stopWords, false);
		}
		
		/// <summary></summary>
		/// <param name="stopWords">An array of stopwords</param>
		/// <param name="ignoreCase">If true, all words are lower cased first.</param>
		/// <returns> a Set containing the words</returns>
		public static ISet<string> MakeStopSet(string[] stopWords, bool ignoreCase)
		{
			var stopSet = new CharArraySet(stopWords.Length, ignoreCase);
		    stopSet.AddAll(stopWords);
			return stopSet;
		}
		
		/// <summary> </summary>
        /// <param name="stopWords">A List of Strings or char[] or any other toString()-able list representing the stopwords </param>
		/// <param name="ignoreCase">if true, all words are lower cased first</param>
		/// <returns>A Set (<see cref="CharArraySet"/>)containing the words</returns>
		public static ISet<string> MakeStopSet(IList<object> stopWords, bool ignoreCase)
		{
			var stopSet = new CharArraySet(stopWords.Count, ignoreCase);
            foreach(var word in stopWords)
                stopSet.Add(word.ToString());
			return stopSet;
		}
		
		/// <summary> Returns the next input Token whose term() is not a stop word.</summary>
		public override bool IncrementToken()
		{
			// return the first non-stop word found
			int skippedPositions = 0;
			while (input.IncrementToken())
			{
				if (!stopWords.Contains(termAtt.TermBuffer(), 0, termAtt.TermLength()))
				{
					if (enablePositionIncrements)
					{
						posIncrAtt.PositionIncrement = posIncrAtt.PositionIncrement + skippedPositions;
					}
					return true;
				}
				skippedPositions += posIncrAtt.PositionIncrement;
			}
			// reached EOS -- return false
			return false;
		}
		
		/// <summary> Returns version-dependent default for enablePositionIncrements. Analyzers
		/// that embed StopFilter use this method when creating the StopFilter. Prior
		/// to 2.9, this returns false. On 2.9 or later, it returns true.
		/// </summary>
		public static bool GetEnablePositionIncrementsVersionDefault(Version matchVersion)
		{
            return matchVersion.OnOrAfter(Version.LUCENE_29);
		}

	    /// <summary> If <c>true</c>, this StopFilter will preserve
	    /// positions of the incoming tokens (ie, accumulate and
	    /// set position increments of the removed stop tokens).
	    /// Generally, <c>true</c> is best as it does not
	    /// lose information (positions of the original tokens)
	    /// during indexing.
	    /// 
	    /// <p/> When set, when a token is stopped
	    /// (omitted), the position increment of the following
	    /// token is incremented.
	    /// 
	    /// <p/> <b>NOTE</b>: be sure to also
	    /// set <see cref="QueryParser.EnablePositionIncrements" /> if
	    /// you use QueryParser to create queries.
	    /// </summary>
	    public bool EnablePositionIncrements
	    {
	        get { return enablePositionIncrements; }
	        set { enablePositionIncrements = value; }
	    }
	}
}