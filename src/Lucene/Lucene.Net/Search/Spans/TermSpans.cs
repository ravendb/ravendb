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
using System.Collections.Generic;
using Lucene.Net.Store;
using Term = Lucene.Net.Index.Term;
using TermPositions = Lucene.Net.Index.TermPositions;

namespace Lucene.Net.Search.Spans
{
	
	/// <summary> Expert:
	/// Public for extension only
	/// </summary>
	public class TermSpans:Spans
	{
		protected internal TermPositions internalPositions;
		protected internal Term term;
		protected internal int internalDoc;
		protected internal int freq;
		protected internal int count;
		protected internal int position;
		
		
		public TermSpans(TermPositions positions, Term term)
		{
			
			this.internalPositions = positions;
			this.term = term;
			internalDoc = - 1;
		}
		
		public override bool Next(IState state)
		{
			if (count == freq)
			{
				if (!internalPositions.Next(state))
				{
					internalDoc = int.MaxValue;
					return false;
				}
				internalDoc = internalPositions.Doc;
				freq = internalPositions.Freq;
				count = 0;
			}
			position = internalPositions.NextPosition(state);
			count++;
			return true;
		}
		
		public override bool SkipTo(int target, IState state)
		{
			if (!internalPositions.SkipTo(target, state))
			{
				internalDoc = int.MaxValue;
				return false;
			}
			
			internalDoc = internalPositions.Doc;
			freq = internalPositions.Freq;
			count = 0;
			
			position = internalPositions.NextPosition(state);
			count++;
			
			return true;
		}
		
		public override int Doc()
		{
			return internalDoc;
		}
		
		public override int Start()
		{
			return position;
		}
		
		public override int End()
		{
			return position + 1;
		}
		
		// TODO: Remove warning after API has been finalized

	    public override ICollection<byte[]> GetPayload(IState state)
	    {
	        byte[] bytes = new byte[internalPositions.PayloadLength];
	        bytes = internalPositions.GetPayload(bytes, 0, state);
	        var val = new System.Collections.Generic.List<byte[]>();
	        val.Add(bytes);
	        return val;
	    }

	    // TODO: Remove warning after API has been finalized

	    public override bool IsPayloadAvailable()
	    {
	        return internalPositions.IsPayloadAvailable;
	    }

	    public override System.String ToString()
		{
			return "spans(" + term.ToString() + ")@" + (internalDoc == - 1?"START":((internalDoc == System.Int32.MaxValue)?"END":internalDoc + "-" + position));
		}

	    public virtual TermPositions Positions
	    {
	        get { return internalPositions; }
	    }
	}
}