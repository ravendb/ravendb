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
	
	/// <summary> {@link Fragmenter} implementation which breaks text up into same-size 
	/// fragments with no concerns over spotting sentence boundaries.
	/// </summary>
	/// <author>  mark@searcharea.co.uk
	/// </author>
	public class SimpleFragmenter : Fragmenter
	{
		private const int DEFAULT_FRAGMENT_SIZE = 100;
		private int currentNumFrags;
		private int fragmentSize;
		
		
		public SimpleFragmenter():this(DEFAULT_FRAGMENT_SIZE)
		{
		}
		
		
		/// <summary> </summary>
		/// <param name="fragmentSize">size in bytes of each fragment
		/// </param>
		public SimpleFragmenter(int fragmentSize)
		{
			this.fragmentSize = fragmentSize;
		}
		
		/* (non-Javadoc)
		* @see Lucene.Net.Highlight.TextFragmenter#start(java.lang.String)
		*/
		public virtual void  Start(System.String originalText)
		{
			currentNumFrags = 1;
		}
		
		/* (non-Javadoc)
		* @see Lucene.Net.Highlight.TextFragmenter#isNewFragment(org.apache.lucene.analysis.Token)
		*/
		public virtual bool IsNewFragment(Token token)
		{
			bool isNewFrag = token.EndOffset() >= (fragmentSize * currentNumFrags);
			if (isNewFrag)
			{
				currentNumFrags++;
			}
			return isNewFrag;
		}
		
		/// <returns> size in bytes of each fragment
		/// </returns>
		public virtual int GetFragmentSize()
		{
			return fragmentSize;
		}
		
		/// <param name="size">size in bytes of each fragment
		/// </param>
		public virtual void  SetFragmentSize(int size)
		{
			fragmentSize = size;
		}
	}
}