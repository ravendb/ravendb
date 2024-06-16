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
using TokenStream = Lucene.Net.Analysis.TokenStream;

namespace Lucene.Net.Analysis.Tokenattributes
{

    /// <summary>The positionIncrement determines the position of this token
    /// relative to the previous Token in a <see cref="TokenStream" />, used in phrase
    /// searching.
    /// 
    /// <p/>The default value is one.
    /// 
    /// <p/>Some common uses for this are:<list>
    /// 
    /// <item>Set it to zero to put multiple terms in the same position.  This is
    /// useful if, e.g., a word has multiple stems.  Searches for phrases
    /// including either stem will match.  In this case, all but the first stem's
    /// increment should be set to zero: the increment of the first instance
    /// should be one.  Repeating a token with an increment of zero can also be
    /// used to boost the scores of matches on that token.</item>
    /// 
    /// <item>Set it to values greater than one to inhibit exact phrase matches.
    /// If, for example, one does not want phrases to match across removed stop
    /// words, then one could build a stop word filter that removes stop words and
    /// also sets the increment to the number of stop words removed before each
    /// non-stop word.  Then exact phrase queries will only match when the terms
    /// occur with no intervening stop words.</item>
    /// 
    /// </list>
    /// </summary>
    [Serializable]
    public sealed class PositionIncrementAttribute:Attribute, IPositionIncrementAttribute, System.ICloneable
	{
		private int positionIncrement = 1;

	    /// <summary>Set the position increment. The default value is one.
	    /// 
	    /// </summary>
	    /// <value> the distance from the prior term </value>
	    public int PositionIncrement
	    {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
	        set { this.positionIncrement = value >= 0 ? value : ThrowIncrementGreaterThanZero(value); }
	        get { return positionIncrement; }
	    }

	    private int ThrowIncrementGreaterThanZero(int value)
	    {
	        throw new System.ArgumentException("Increment must be zero or greater: " + value);
        }

	    public override void  Clear()
		{
			ClearFast();
		}

	    // PERF: When CoreCLR 2.0 this can be replaced for Clear with AggresiveInlining because of devirtualization.
	    [MethodImpl(MethodImplOptions.AggressiveInlining)]
	    public void ClearFast()
	    {
	        this.positionIncrement = 1;
	    }

        public  override bool Equals(System.Object other)
		{
			if (other == this)
			{
				return true;
			}
			
			if (other is PositionIncrementAttribute)
			{
				return positionIncrement == ((PositionIncrementAttribute) other).positionIncrement;
			}
			
			return false;
		}
		
		public override int GetHashCode()
		{
			return positionIncrement;
		}
		
		public override void  CopyTo(Attribute target)
		{
			IPositionIncrementAttribute t = (IPositionIncrementAttribute) target;
			t.PositionIncrement = positionIncrement;
		}
		
		public override Object Clone()
		{
            PositionIncrementAttribute impl = new PositionIncrementAttribute();
            impl.positionIncrement = positionIncrement;
            return impl;
		}

    }
}