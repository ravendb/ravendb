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
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using SF.Snowball.Ext;
namespace Lucene.Net.Analysis.Snowball
{
	
	/// <summary>Filters {@link StandardTokenizer} with {@link StandardFilter}, {@link
	/// LowerCaseFilter}, {@link StopFilter} and {@link SnowballFilter}.
	/// 
	/// Available stemmers are listed in {@link SF.Snowball.Ext}.  The name of a
	/// stemmer is the part of the class name before "Stemmer", e.g., the stemmer in
	/// {@link EnglishStemmer} is named "English".
	/// </summary>
	public class SnowballAnalyzer : Analyzer
	{
		private System.String name;
		private System.Collections.Hashtable stopSet;
		
		/// <summary>Builds the named analyzer with no stop words. </summary>
		public SnowballAnalyzer(System.String name)
		{
			this.name = name;
		}
		
		/// <summary>Builds the named analyzer with the given stop words. </summary>
		public SnowballAnalyzer(System.String name, System.String[] stopWords) : this(name)
		{
			stopSet = StopFilter.MakeStopSet(stopWords);
		}
		
		/// <summary>Constructs a {@link StandardTokenizer} filtered by a {@link
		/// StandardFilter}, a {@link LowerCaseFilter} and a {@link StopFilter}. 
		/// </summary>
        public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
		{
			TokenStream result = new StandardTokenizer(reader);
			result = new StandardFilter(result);
			result = new LowerCaseFilter(result);
			if (stopSet != null)
				result = new StopFilter(result, stopSet);
			result = new SnowballFilter(result, name);
			return result;
		}
	}
}