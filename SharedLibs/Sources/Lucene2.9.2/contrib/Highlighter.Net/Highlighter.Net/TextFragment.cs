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
	/// <summary> Low-level class used to record information about a section of a document 
	/// with a score.
	/// </summary>
	/// <author>  MAHarwood
	/// 
	/// 
	/// </author>
	public class TextFragment
	{
		internal System.Text.StringBuilder markedUpText;
		internal int fragNum;
		internal int textStartPos;
		internal int textEndPos;
		internal float score;
		
		public TextFragment(System.Text.StringBuilder markedUpText, int textStartPos, int fragNum)
		{
			this.markedUpText = markedUpText;
			this.textStartPos = textStartPos;
			this.fragNum = fragNum;
		}
		internal virtual void  SetScore(float score)
		{
			this.score = score;
		}
		public virtual float GetScore()
		{
			return score;
		}
		/// <param name="frag2">Fragment to be merged into this one
		/// </param>
		public virtual void  Merge(TextFragment frag2)
		{
			textEndPos = frag2.textEndPos;
			score = System.Math.Max(score, frag2.score);
		}
		/// <param name="fragment">
		/// </param>
		/// <returns> true if this fragment follows the one passed
		/// </returns>
		public virtual bool Follows(TextFragment fragment)
		{
			return textStartPos == fragment.textEndPos;
		}
		
		/// <returns> the fragment sequence number
		/// </returns>
		public virtual int GetFragNum()
		{
			return fragNum;
		}
		
		/* Returns the marked-up text for this text fragment 
		*/
		public override System.String ToString()
		{
			return markedUpText.ToString(textStartPos, textEndPos - textStartPos);
		}
	}
}