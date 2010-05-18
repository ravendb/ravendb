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

namespace Lucene.Net.Highlight
{
	/// <summary> Simple {@link Encoder} implementation to escape text for HTML output</summary>
	/// <author>  Nicko Cadell
	/// 
	/// </author>
	public class SimpleHTMLEncoder : Encoder
	{
		public SimpleHTMLEncoder()
		{
		}
		
		public virtual System.String EncodeText(System.String originalText)
		{
			return HtmlEncode(originalText);
		}
		
		/// <summary> Encode string into HTML</summary>
		public static System.String HtmlEncode(System.String plainText)
		{
			if (plainText == null || plainText.Length == 0)
			{
				return "";
			}
			
			System.Text.StringBuilder result = new System.Text.StringBuilder(plainText.Length);
			
			for (int index = 0; index < plainText.Length; index++)
			{
				char ch = plainText[index];
				
				switch (ch)
				{
					
					case '"': 
						result.Append("&quot;");
						break;
					
					
					case '&': 
						result.Append("&amp;");
						break;
					
					
					case '<': 
						result.Append("&lt;");
						break;
					
					
					case '>': 
						result.Append("&gt;");
						break;
					
					
					default: 
						if (ch < 128)
						{
							result.Append(ch);
						}
						else
						{
							result.Append("&#").Append((int) ch).Append(";");
						}
						break;
					
				}
			}
			
			return result.ToString();
		}
	}
}