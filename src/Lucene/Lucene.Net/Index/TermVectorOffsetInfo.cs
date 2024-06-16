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

using System.Runtime.InteropServices;

namespace Lucene.Net.Index
{

    /// <summary> The TermVectorOffsetInfo class holds information pertaining to a Term in a <see cref="Lucene.Net.Index.TermPositionVector" />'s
    /// offset information.  This offset information is the character offset as set during the Analysis phase (and thus may not be the actual offset in the
    /// original content).
    /// </summary>
    [Serializable]
    public struct TermVectorOffsetInfo : IEquatable<TermVectorOffsetInfo>
	{
		/// <summary> Convenience declaration when creating a <see cref="Lucene.Net.Index.TermPositionVector" /> that stores only position information.</summary>
		[NonSerialized]
		public static readonly TermVectorOffsetInfo[] EMPTY_OFFSET_INFO = new TermVectorOffsetInfo[0];
        
		[NonSerialized]
        public static readonly TermVectorOffsetInfo Null = new TermVectorOffsetInfo(int.MinValue, int.MinValue);

		private int startOffset;
		private int endOffset;
		
        //public TermVectorOffsetInfo()
        //{
        //}
		
		public TermVectorOffsetInfo(int startOffset, int endOffset)
		{
			this.endOffset = endOffset;
			this.startOffset = startOffset;
		}

	    /// <summary> The accessor for the ending offset for the term</summary>
	    /// <value> The offset </value>
	    public int EndOffset
	    {
	        get { return endOffset; }
	        set { this.endOffset = value; }
	    }

	    /// <summary> The accessor for the starting offset of the term.
	    /// 
	    /// </summary>
	    /// <value> The offset </value>
	    public int StartOffset
	    {
	        get { return startOffset; }
	        set { this.startOffset = value; }
	    }

        ///// <summary> Two TermVectorOffsetInfos are equals if both the start and end offsets are the same</summary>
        ///// <param name="o">The comparison Object
        ///// </param>
        ///// <returns> true if both <see cref="GetStartOffset()" /> and <see cref="GetEndOffset()" /> are the same for both objects.
        ///// </returns>
        //public  override bool Equals(System.Object o)
        //{
        //    if (this == o)
        //        return true;
        //    if (!(o is TermVectorOffsetInfo))
        //        return false;
			
        //    TermVectorOffsetInfo termVectorOffsetInfo = (TermVectorOffsetInfo) o;
			
        //    if (endOffset != termVectorOffsetInfo.endOffset)
        //        return false;
        //    if (startOffset != termVectorOffsetInfo.startOffset)
        //        return false;
			
        //    return true;
        //}
		
        //public override int GetHashCode()
        //{
        //    int result;
        //    result = startOffset;
        //    result = 29 * result + endOffset;
        //    return result;
        //}


	    public bool Equals(TermVectorOffsetInfo other)
	    {
	        return startOffset == other.startOffset && endOffset == other.endOffset;
	    }

	    public override bool Equals(object obj)
	    {
	        if (ReferenceEquals(null, obj))
	        {
	            return EndOffset == int.MinValue && StartOffset == int.MinValue;
	        }
	        if (obj.GetType() != typeof (TermVectorOffsetInfo)) return false;
	        return Equals((TermVectorOffsetInfo) obj);
	    }

	    public override int GetHashCode()
	    {
	        unchecked
	        {
	            return (startOffset*397) ^ endOffset;
	        }
	    }

	    public static bool operator ==(TermVectorOffsetInfo left, object right)
	    {
	        return left.Equals(right);
	    }

        public static bool operator !=(TermVectorOffsetInfo left, object right)
	    {
	        return !left.Equals(right);
	    }
	}
}