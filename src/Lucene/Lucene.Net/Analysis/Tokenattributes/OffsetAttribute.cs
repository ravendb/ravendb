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
using System.Runtime.CompilerServices;
using Attribute = Lucene.Net.Util.Attribute;

namespace Lucene.Net.Analysis.Tokenattributes
{

    /// <summary> The start and end character offset of a Token. </summary>
    [Serializable]
    public sealed class OffsetAttribute:Attribute, IOffsetAttribute, System.ICloneable
	{
		private int startOffset;
		private int endOffset;

	    /// <summary>Returns this Token's starting offset, the position of the first character
	    /// corresponding to this token in the source text.
	    /// Note that the difference between endOffset() and startOffset() may not be
	    /// equal to termText.length(), as the term text may have been altered by a
	    /// stemmer or some other filter. 
	    /// </summary>
	    public int StartOffset
	    {
	        get { return startOffset; }
	    }


	    /// <summary>Set the starting and ending offset.
        /// See StartOffset() and EndOffset()
        /// </summary>
		public void SetOffset(int startOffset, int endOffset)
		{
			this.startOffset = startOffset;
			this.endOffset = endOffset;
		}


	    /// <summary>Returns this Token's ending offset, one greater than the position of the
	    /// last character corresponding to this token in the source text. The length
	    /// of the token in the source text is (endOffset - startOffset). 
	    /// </summary>
	    public int EndOffset
	    {
	        get { return endOffset; }
	    }


	    public override void  Clear()
		{
		    ClearFast();
        }

	    // PERF: When CoreCLR 2.0 this can be replaced for Clear with AggresiveInlining because of devirtualization.
	    [MethodImpl(MethodImplOptions.AggressiveInlining)]
	    public void ClearFast()
	    {
	        startOffset = 0;
	        endOffset = 0;
	    }

        public  override bool Equals(System.Object other)
		{
			if (other == this)
			{
				return true;
			}
			
			if (other is OffsetAttribute)
			{
				OffsetAttribute o = (OffsetAttribute) other;
				return o.startOffset == startOffset && o.endOffset == endOffset;
			}
			
			return false;
		}
		
		public override int GetHashCode()
		{
			int code = startOffset;
			code = code * 31 + endOffset;
			return code;
		}
		
		public override void  CopyTo(Attribute target)
		{
			IOffsetAttribute t = (IOffsetAttribute) target;
			t.SetOffset(startOffset, endOffset);
		}
		
		public override System.Object Clone()
		{
            OffsetAttribute impl = new OffsetAttribute();
            impl.endOffset = endOffset;
            impl.startOffset = startOffset;
            return impl;
		}
	}
}