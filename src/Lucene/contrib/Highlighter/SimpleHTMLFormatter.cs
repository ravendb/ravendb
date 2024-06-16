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

namespace Lucene.Net.Search.Highlight
{
	/// <summary> Simple <see cref="IFormatter"/> implementation to highlight terms with a pre and post tag</summary>
	/// <author>  MAHarwood
	/// 
	/// </author>
	public class SimpleHTMLFormatter : IFormatter
	{
		internal System.String preTag;
		internal System.String postTag;
		
		
		public SimpleHTMLFormatter(System.String preTag, System.String postTag)
		{
			this.preTag = preTag;
			this.postTag = postTag;
		}
		
		/// <summary> Default constructor uses HTML: &lt;B&gt; tags to markup terms
		/// 
		/// 
		/// </summary>
		public SimpleHTMLFormatter()
		{
			this.preTag = "<B>";
			this.postTag = "</B>";
		}
		
		/* (non-Javadoc)
		* <see cref="Lucene.Net.Highlight.Formatter.highlightTerm(java.lang.String, Lucene.Net.Highlight.TokenGroup)"/>
		*/
		public virtual System.String HighlightTerm(System.String originalText, TokenGroup tokenGroup)
		{
			System.Text.StringBuilder returnBuffer;
			if (tokenGroup.TotalScore > 0)
			{
				returnBuffer = new System.Text.StringBuilder();
				returnBuffer.Append(preTag);
				returnBuffer.Append(originalText);
				returnBuffer.Append(postTag);
				return returnBuffer.ToString();
			}
			return originalText;
		}
	}
}