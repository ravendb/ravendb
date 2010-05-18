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
	
	/// <summary> Implements the policy for breaking text into multiple fragments for consideration
	/// by the {@link Highlighter} class. A sophisticated implementation may do this on the basis
	/// of detecting end of sentences in the text. 
	/// </summary>
	/// <author>  mark@searcharea.co.uk
	/// </author>
	public interface Fragmenter
	{
		/// <summary> Initializes the Fragmenter</summary>
		/// <param name="">originalText
		/// </param>
		void  Start(System.String originalText);
		
		/// <summary> Test to see if this token from the stream should be held in a new TextFragment</summary>
		/// <param name="">nextToken
		/// </param>
		bool IsNewFragment(Token nextToken);
	}
}