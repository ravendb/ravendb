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
using TokenFilter = Lucene.Net.Analysis.TokenFilter;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using SnowballProgram = SF.Snowball.SnowballProgram;
using SF.Snowball.Ext;

namespace Lucene.Net.Analysis.Snowball
{
	
	/// <summary>A filter that stems words using a Snowball-generated stemmer.
	/// 
	/// Available stemmers are listed in {@link SF.Snowball.Ext}.  The name of a
	/// stemmer is the part of the class name before "Stemmer", e.g., the stemmer in
	/// {@link EnglishStemmer} is named "English".
	/// </summary>
	
	public class SnowballFilter : TokenFilter
	{
		private static readonly System.Object[] EMPTY_ARGS = new System.Object[0];
		
		private SnowballProgram stemmer;
		private System.Reflection.MethodInfo stemMethod;
		
		/// <summary>Construct the named stemming filter.
		/// 
		/// </summary>
		/// <param name="in">the input tokens to stem
		/// </param>
		/// <param name="name">the name of a stemmer
		/// </param>
		public SnowballFilter(TokenStream in_Renamed, System.String name) : base(in_Renamed)
		{
			try
			{
				System.Type stemClass = System.Type.GetType("SF.Snowball.Ext." + name + "Stemmer");
				stemmer = (SnowballProgram) System.Activator.CreateInstance(stemClass);
				// why doesn't the SnowballProgram class have an (abstract?) stem method?
				stemMethod = stemClass.GetMethod("Stem", (new System.Type[0] == null) ? new System.Type[0] : (System.Type[]) new System.Type[0]);
			}
			catch (System.Exception e)
			{
				throw new System.SystemException(e.ToString());
			}
		}
		
		/// <summary>Returns the next input Token, after being stemmed </summary>
        public override Token Next()
		{
			Token token = input.Next();
			if (token == null)
				return null;
			stemmer.SetCurrent(token.TermText());
			try
			{
				stemMethod.Invoke(stemmer, (System.Object[]) EMPTY_ARGS);
			}
			catch (System.Exception e)
			{
				throw new System.SystemException(e.ToString());
			}
			
			Token newToken = new Token(stemmer.GetCurrent(), token.StartOffset(), token.EndOffset(), token.Type());
			newToken.SetPositionIncrement(token.GetPositionIncrement());
			return newToken;
		}
	}
}