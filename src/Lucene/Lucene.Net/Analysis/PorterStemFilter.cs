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

using Lucene.Net.Analysis.Tokenattributes;

namespace Lucene.Net.Analysis
{
	
	/// <summary>Transforms the token stream as per the Porter stemming algorithm.
	/// Note: the input to the stemming filter must already be in lower case,
	/// so you will need to use LowerCaseFilter or LowerCaseTokenizer farther
	/// down the Tokenizer chain in order for this to work properly!
	/// <p/>
	/// To use this filter with other analyzers, you'll want to write an
	/// Analyzer class that sets up the TokenStream chain as you want it.
	/// To use this with LowerCaseTokenizer, for example, you'd write an
	/// analyzer like this:
	/// <p/>
	/// <code>
	/// class MyAnalyzer extends Analyzer {
	///     public final TokenStream tokenStream(String fieldName, Reader reader) {
	///          return new PorterStemFilter(new LowerCaseTokenizer(reader));
	///     }
	/// }
	/// </code>
	/// </summary>
	public sealed class PorterStemFilter:TokenFilter
	{
		private readonly PorterStemmer stemmer;
		private readonly ITermAttribute termAtt;
		
		public PorterStemFilter(TokenStream in_Renamed):base(in_Renamed)
		{
			stemmer = new PorterStemmer();
            termAtt = AddAttribute<ITermAttribute>();
		}
		
		public override bool IncrementToken()
		{
			if (!input.IncrementToken())
				return false;
			
			if (stemmer.Stem(termAtt.TermBuffer(), 0, termAtt.TermLength()))
				termAtt.SetTermBuffer(stemmer.ResultBuffer, 0, stemmer.ResultLength);
			return true;
		}
	}
}