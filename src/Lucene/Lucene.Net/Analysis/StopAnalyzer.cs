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

using System.Collections.Generic;
using Version = Lucene.Net.Util.Version;

namespace Lucene.Net.Analysis
{
	
	/// <summary> Filters <see cref="LetterTokenizer" /> with <see cref="LowerCaseFilter" /> and
	/// <see cref="StopFilter" />.
	/// 
	/// <a name="version"/>
	/// <p/>
	/// You must specify the required <see cref="Version" /> compatibility when creating
	/// StopAnalyzer:
	/// <list type="bullet">
	/// <item>As of 2.9, position increments are preserved</item>
	/// </list>
	/// </summary>
	
	public sealed class StopAnalyzer:Analyzer
	{
		private readonly ISet<string> stopWords;
		private readonly bool enablePositionIncrements;

		/// <summary>An unmodifiable set containing some common English words that are not usually useful
		/// for searching.
		/// </summary>
        public static ISet<string> ENGLISH_STOP_WORDS_SET;
		
		/// <summary> Builds an analyzer which removes words in ENGLISH_STOP_WORDS.</summary>
		public StopAnalyzer(Version matchVersion)
		{
			stopWords = ENGLISH_STOP_WORDS_SET;
			enablePositionIncrements = StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion);
		}

		/// <summary>Builds an analyzer with the stop words from the given set.</summary>
		public StopAnalyzer(Version matchVersion, ISet<string> stopWords)
		{
			this.stopWords = stopWords;
			enablePositionIncrements = StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion);
		}
		
		/// <summary> Builds an analyzer with the stop words from the given file.
		/// 
		/// </summary>
		/// <seealso cref="WordlistLoader.GetWordSet(System.IO.FileInfo)">
		/// </seealso>
		/// <param name="matchVersion">See <a href="#version">above</a>
		/// </param>
		/// <param name="stopwordsFile">File to load stop words from
		/// </param>
		public StopAnalyzer(Version matchVersion, System.IO.FileInfo stopwordsFile)
		{
			stopWords = WordlistLoader.GetWordSet(stopwordsFile);
			enablePositionIncrements = StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion);
		}

        /// <summary>Builds an analyzer with the stop words from the given reader. </summary>
        /// <seealso cref="WordlistLoader.GetWordSet(System.IO.TextReader)">
        /// </seealso>
        /// <param name="matchVersion">See <a href="#Version">above</a>
        /// </param>
        /// <param name="stopwords">Reader to load stop words from
        /// </param>
        public StopAnalyzer(Version matchVersion, System.IO.TextReader stopwords)
        {
            stopWords = WordlistLoader.GetWordSet(stopwords);
            enablePositionIncrements = StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion);
        }

        /// <summary>Filters LowerCaseTokenizer with StopFilter. </summary>
		public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
		{
			return new StopFilter(enablePositionIncrements, new LowerCaseTokenizer(reader), stopWords);
		}
		
		/// <summary>Filters LowerCaseTokenizer with StopFilter. </summary>
		private class SavedStreams
		{
			public SavedStreams(StopAnalyzer enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(StopAnalyzer enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private StopAnalyzer enclosingInstance;
			public StopAnalyzer Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal Tokenizer source;
			internal TokenStream result;
		}
		
		public override TokenStream ReusableTokenStream(System.String fieldName, System.IO.TextReader reader)
		{
			var streams = (SavedStreams) PreviousTokenStream;
			if (streams == null)
			{
				streams = new SavedStreams(this) {source = new LowerCaseTokenizer(reader)};
				streams.result = new StopFilter(enablePositionIncrements, streams.source, stopWords);
				PreviousTokenStream = streams;
			}
			else
				streams.source.Reset(reader);
			return streams.result;
		}
		static StopAnalyzer()
		{
			{
				var stopWords = new System.String[]{"a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these", "they", "this", "to", "was", "will", "with"};
				var stopSet = new CharArraySet(stopWords.Length, false);
				stopSet.AddAll(stopWords);
				ENGLISH_STOP_WORDS_SET = CharArraySet.UnmodifiableSet(stopSet);
			}
		}
	}
}