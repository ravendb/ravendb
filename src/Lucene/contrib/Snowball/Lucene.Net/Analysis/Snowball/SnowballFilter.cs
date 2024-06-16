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
using Lucene.Net.Analysis.Tokenattributes;
using Token = Lucene.Net.Analysis.Token;
using TokenFilter = Lucene.Net.Analysis.TokenFilter;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using SnowballProgram = SF.Snowball.SnowballProgram;
using SF.Snowball.Ext;

namespace Lucene.Net.Analysis.Snowball
{
	
	/// <summary>A filter that stems words using a Snowball-generated stemmer.
	/// 
	/// Available stemmers are listed in <see cref="SF.Snowball.Ext"/>.  The name of a
	/// stemmer is the part of the class name before "Stemmer", e.g., the stemmer in
	/// <see cref="EnglishStemmer"/> is named "English".
	/// </summary>
	
	public sealed class SnowballFilter : TokenFilter
	{
		private static readonly System.Object[] EMPTY_ARGS = new System.Object[0];
		
		private SnowballProgram stemmer;
	    private ITermAttribute termAtt;
		//private System.Reflection.MethodInfo stemMethod;

	    public SnowballFilter(TokenStream input, SnowballProgram stemmer)
            : base(input)
	    {
	        this.stemmer = stemmer;
            termAtt = AddAttribute<ITermAttribute>();
	    }

		/// <summary>Construct the named stemming filter.
		/// 
		/// </summary>
        /// <param name="input">the input tokens to stem
		/// </param>
		/// <param name="name">the name of a stemmer
		/// </param>
		public SnowballFilter(TokenStream input, System.String name) : base(input)
		{
			try
			{
				System.Type stemClass = System.Type.GetType("SF.Snowball.Ext." + name + "Stemmer");
				stemmer = (SnowballProgram) System.Activator.CreateInstance(stemClass);
			}
			catch (System.Exception e)
			{
				throw new System.SystemException(e.ToString());
			}
		    termAtt = AddAttribute<ITermAttribute>();
		}
		
		/// <summary>Returns the next input Token, after being stemmed </summary>
        public sealed override bool IncrementToken()
		{
            if (input.IncrementToken())
            {
                String originalTerm = termAtt.Term;
                stemmer.SetCurrent(originalTerm);
                stemmer.Stem();
                String finalTerm = stemmer.GetCurrent();
                // Don't bother updating, if it is unchanged.
                if (!originalTerm.Equals(finalTerm))
                    termAtt.SetTermBuffer(finalTerm);
                return true;
            }
            else
            {
                return false;
            }
		}
	}
}