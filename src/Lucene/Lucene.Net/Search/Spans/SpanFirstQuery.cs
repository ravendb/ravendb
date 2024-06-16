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
using Lucene.Net.Index;
using Lucene.Net.Store;
using Lucene.Net.Support;
using IndexReader = Lucene.Net.Index.IndexReader;
using ToStringUtils = Lucene.Net.Util.ToStringUtils;
using Query = Lucene.Net.Search.Query;

namespace Lucene.Net.Search.Spans
{

    /// <summary>Matches spans near the beginning of a field. </summary>
    [Serializable]
    public class SpanFirstQuery : SpanQuery, System.ICloneable
	{
		private class AnonymousClassSpans : Spans
		{
			public AnonymousClassSpans(Lucene.Net.Index.IndexReader reader, SpanFirstQuery enclosingInstance, IState state)
			{
				InitBlock(reader, enclosingInstance, state);
			}
			private void  InitBlock(Lucene.Net.Index.IndexReader reader, SpanFirstQuery enclosingInstance, IState state)
			{
				this.reader = reader;
				this.enclosingInstance = enclosingInstance;
				spans = Enclosing_Instance.match.GetSpans(reader, state);
			}
			private Lucene.Net.Index.IndexReader reader;
			private SpanFirstQuery enclosingInstance;
			public SpanFirstQuery Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private Spans spans;
			
			public override bool Next(IState state)
			{
				while (spans.Next(state))
				{
					// scan to next match
					if (End() <= Enclosing_Instance.end)
						return true;
				}
				return false;
			}
			
			public override bool SkipTo(int target, IState state)
			{
				if (!spans.SkipTo(target, state))
					return false;
				
				return spans.End() <= Enclosing_Instance.end || Next(state);
			}
			
			public override int Doc()
			{
				return spans.Doc();
			}
			public override int Start()
			{
				return spans.Start();
			}
			public override int End()
			{
				return spans.End();
			}
			
			// TODO: Remove warning after API has been finalized

		    public override ICollection<byte[]> GetPayload(IState state)
		    {
		        System.Collections.Generic.ICollection<byte[]> result = null;
		        if (spans.IsPayloadAvailable())
		        {
		            result = spans.GetPayload(state);
		        }
		        return result; //TODO: any way to avoid the new construction?
		    }

		    // TODO: Remove warning after API has been finalized

		    public override bool IsPayloadAvailable()
		    {
		        return spans.IsPayloadAvailable();
		    }

		    public override System.String ToString()
			{
				return "spans(" + Enclosing_Instance.ToString() + ")";
			}
		}
		private SpanQuery match;
		private int end;
		
		/// <summary>Construct a SpanFirstQuery matching spans in <c>match</c> whose end
		/// position is less than or equal to <c>end</c>. 
		/// </summary>
		public SpanFirstQuery(SpanQuery match, int end)
		{
			this.match = match;
			this.end = end;
		}

	    /// <summary>Return the SpanQuery whose matches are filtered. </summary>
	    public virtual SpanQuery Match
	    {
	        get { return match; }
	    }

	    /// <summary>Return the maximum end position permitted in a match. </summary>
	    public virtual int End
	    {
	        get { return end; }
	    }

	    public override string Field
	    {
	        get { return match.Field; }
	    }

	    public override System.String ToString(System.String field)
		{
			System.Text.StringBuilder buffer = new System.Text.StringBuilder();
			buffer.Append("spanFirst(");
			buffer.Append(match.ToString(field));
			buffer.Append(", ");
			buffer.Append(end);
			buffer.Append(")");
			buffer.Append(ToStringUtils.Boost(Boost));
			return buffer.ToString();
		}
		
		public override System.Object Clone()
		{
			SpanFirstQuery spanFirstQuery = new SpanFirstQuery((SpanQuery) match.Clone(), end);
			spanFirstQuery.Boost = Boost;
			return spanFirstQuery;
		}
		
		public override void  ExtractTerms(System.Collections.Generic.ISet<Term> terms)
		{
			match.ExtractTerms(terms);
		}
		
		public override Spans GetSpans(IndexReader reader, IState state)
		{
			return new AnonymousClassSpans(reader, this, state);
		}
		
		public override Query Rewrite(IndexReader reader, IState state)
		{
			SpanFirstQuery clone = null;
			
			SpanQuery rewritten = (SpanQuery) match.Rewrite(reader, state);
			if (rewritten != match)
			{
				clone = (SpanFirstQuery) this.Clone();
				clone.match = rewritten;
			}
			
			if (clone != null)
			{
				return clone; // some clauses rewrote
			}
			else
			{
				return this; // no clauses rewrote
			}
		}
		
		public  override bool Equals(System.Object o)
		{
			if (this == o)
				return true;
			if (!(o is SpanFirstQuery))
				return false;
			
			SpanFirstQuery other = (SpanFirstQuery) o;
			return this.end == other.end && this.match.Equals(other.match) && this.Boost == other.Boost;
		}
		
		public override int GetHashCode()
		{
			int h = match.GetHashCode();
			h ^= ((h << 8) | (Number.URShift(h, 25))); // reversible
			h ^= System.Convert.ToInt32(Boost) ^ end;
			return h;
		}
	}
}