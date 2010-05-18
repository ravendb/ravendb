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
using Token = Lucene.Net.Analysis.Token;

namespace Lucene.Net.Highlight
{
	
	/// <summary> Adds to the score for a fragment based on its tokens</summary>
	/// <author>  mark@searcharea.co.uk
	/// </author>
	public interface Scorer
	{
		/// <summary> called when a new fragment is started for consideration</summary>
		/// <param name="">newFragment
		/// </param>
		void  StartFragment(TextFragment newFragment);
		
		/// <summary> Called for each token in the current fragment</summary>
		/// <param name="token">The token to be scored
		/// </param>
		/// <returns> a score which is passed to the Highlighter class to influence the mark-up of the text
		/// (this return value is NOT used to score the fragment)
		/// </returns>
		float GetTokenScore(Token token);
		
		
		/// <summary> Called when the highlighter has no more tokens for the current fragment - the scorer returns
		/// the weighting it has derived for the most recent fragment, typically based on the tokens
		/// passed to getTokenScore(). 
		/// 
		/// </summary>
		float GetFragmentScore();
	}
}