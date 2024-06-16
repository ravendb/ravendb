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

namespace Lucene.Net.Analysis
{
	
	/// <summary> This class can be used if the token attributes of a TokenStream
	/// are intended to be consumed more than once. It caches
	/// all token attribute states locally in a List.
	/// 
	/// <p/>CachingTokenFilter implements the optional method
	/// <see cref="TokenStream.Reset()" />, which repositions the
	/// stream to the first Token. 
	/// </summary>
	public sealed class CachingTokenFilter : TokenFilter
	{
        private System.Collections.Generic.LinkedList<State> cache = null;
		private System.Collections.Generic.IEnumerator<State> iterator = null;
		private State finalState;
		
		public CachingTokenFilter(TokenStream input):base(input)
		{
		}

		public override bool IncrementToken()
		{
			if (cache == null)
			{
				// fill cache lazily
				cache = new System.Collections.Generic.LinkedList<State>();
				FillCache();
				iterator = cache.GetEnumerator();
			}
			
			if (!iterator.MoveNext())
			{
				// the cache is exhausted, return false
				return false;
			}
			// Since the TokenFilter can be reset, the tokens need to be preserved as immutable.
			RestoreState(iterator.Current);
			return true;
		}
		
		public override void  End()
		{
			if (finalState != null)
			{
				RestoreState(finalState);
			}
		}
		
		public override void  Reset()
		{
			if (cache != null)
			{
				iterator = cache.GetEnumerator();
			}
		}
		
		private void  FillCache()
		{
			while (input.IncrementToken())
			{
				cache.AddLast(CaptureState());
			}
			// capture final state
			input.End();
			finalState = CaptureState();
		}
	}
}