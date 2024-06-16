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

using System.IO;
using Lucene.Net.Analysis;

namespace Lucene.Net.Search.Highlight
{
    /// <summary> Adds to the score for a fragment based on its tokens</summary>
    public interface IScorer
    {
        /// <summary>
        /// Called to init the Scorer with a {@link TokenStream}. You can grab references to
        /// the attributes you are interested in here and access them from {@link #getTokenScore()}.
        /// </summary>
        /// <param name="tokenStream">the {@link TokenStream} that will be scored.</param>
        /// <returns>
        /// either a {@link TokenStream} that the Highlighter should continue using (eg
        /// if you read the tokenSream in this method) or null to continue
        /// using the same {@link TokenStream} that was passed in.
        /// </returns> 
        /// <exception cref="IOException"></exception>
        ///
        TokenStream Init(TokenStream tokenStream);

        /// <summary>
        /// Called when a new fragment is started for consideration.
        /// </summary>
        /// <param name="newFragment">the fragment that will be scored next</param>
        void StartFragment(TextFragment newFragment);

        /// <summary>
        /// Called for each token in the current fragment. The {@link Highlighter} will
        /// increment the {@link TokenStream} passed to init on every call.
        /// </summary>
        /// <returns>a score which is passed to the {@link Highlighter} class to influence the
        /// mark-up of the text (this return value is NOT used to score the
        /// fragment)</returns> 
        float GetTokenScore();

        ///<summary>
        /// Called when the {@link Highlighter} has no more tokens for the current fragment -
        /// the Scorer returns the weighting it has derived for the most recent
        /// fragment, typically based on the results of {@link #getTokenScore()}.
        /// </summary>
        float FragmentScore { get; }
    }
}