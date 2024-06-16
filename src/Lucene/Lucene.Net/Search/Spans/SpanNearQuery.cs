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
using System.Linq;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Lucene.Net.Support;
using IndexReader = Lucene.Net.Index.IndexReader;
using ToStringUtils = Lucene.Net.Util.ToStringUtils;
using Query = Lucene.Net.Search.Query;

namespace Lucene.Net.Search.Spans
{

    /// <summary>Matches spans which are near one another.  One can specify <i>slop</i>, the
    /// maximum number of intervening unmatched positions, as well as whether
    /// matches are required to be in-order. 
    /// </summary>
    [Serializable]
    public class SpanNearQuery : SpanQuery, System.ICloneable
	{
		protected internal System.Collections.Generic.IList<SpanQuery> clauses;
		protected internal int internalSlop;
		protected internal bool inOrder;
		
		protected internal System.String internalField;
		private readonly bool collectPayloads;
		
		/// <summary>Construct a SpanNearQuery.  Matches spans matching a span from each
		/// clause, with up to <c>slop</c> total unmatched positions between
		/// them.  * When <c>inOrder</c> is true, the spans from each clause
		/// must be * ordered as in <c>clauses</c>. 
		/// </summary>
		public SpanNearQuery(SpanQuery[] clauses, int slop, bool inOrder):this(clauses, slop, inOrder, true)
		{
		}
		
		public SpanNearQuery(SpanQuery[] clauses, int slop, bool inOrder, bool collectPayloads)
		{
			
			// copy clauses array into an ArrayList
			this.clauses = new System.Collections.Generic.List<SpanQuery>(clauses.Length);
			for (int i = 0; i < clauses.Length; i++)
			{
				SpanQuery clause = clauses[i];
				if (i == 0)
				{
					// check field
					internalField = clause.Field;
				}
				else if (!clause.Field.Equals(internalField))
				{
					throw new System.ArgumentException("Clauses must have same field.");
				}
				this.clauses.Add(clause);
			}
			this.collectPayloads = collectPayloads;
			this.internalSlop = slop;
			this.inOrder = inOrder;
		}
		
		/// <summary>Return the clauses whose spans are matched. </summary>
		public virtual SpanQuery[] GetClauses()
		{
            // Return a copy
			return clauses.ToArray();
		}

	    /// <summary>Return the maximum number of intervening unmatched positions permitted.</summary>
	    public virtual int Slop
	    {
	        get { return internalSlop; }
	    }

	    /// <summary>Return true if matches are required to be in-order.</summary>
	    public virtual bool IsInOrder
	    {
	        get { return inOrder; }
	    }

	    public override string Field
	    {
	        get { return internalField; }
	    }

	    public override void  ExtractTerms(System.Collections.Generic.ISet<Term> terms)
		{
            foreach (SpanQuery clause in clauses)
            {
                clause.ExtractTerms(terms);
            }
		}

		public override System.String ToString(System.String field)
		{
			System.Text.StringBuilder buffer = new System.Text.StringBuilder();
			buffer.Append("spanNear([");
			System.Collections.Generic.IEnumerator<SpanQuery> i = clauses.GetEnumerator();
			while (i.MoveNext())
			{
				SpanQuery clause = i.Current;
				buffer.Append(clause.ToString(field));
                buffer.Append(", ");
			}
            if (clauses.Count > 0) buffer.Length -= 2;
			buffer.Append("], ");
			buffer.Append(internalSlop);
			buffer.Append(", ");
			buffer.Append(inOrder);
			buffer.Append(")");
			buffer.Append(ToStringUtils.Boost(Boost));
			return buffer.ToString();
		}
		
		public override Spans GetSpans(IndexReader reader, IState state)
		{
			if (clauses.Count == 0)
			// optimize 0-clause case
				return new SpanOrQuery(GetClauses()).GetSpans(reader, state);
			
			if (clauses.Count == 1)
			// optimize 1-clause case
				return clauses[0].GetSpans(reader, state);
			
			return inOrder?(Spans) new NearSpansOrdered(this, reader, collectPayloads, state):(Spans) new NearSpansUnordered(this, reader, state);
		}
		
		public override Query Rewrite(IndexReader reader, IState state)
		{
			SpanNearQuery clone = null;
			for (int i = 0; i < clauses.Count; i++)
			{
				SpanQuery c = clauses[i];
				SpanQuery query = (SpanQuery) c.Rewrite(reader, state);
				if (query != c)
				{
					// clause rewrote: must clone
					if (clone == null)
						clone = (SpanNearQuery) this.Clone();
					clone.clauses[i] = query;
				}
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
		
		public override System.Object Clone()
		{
			int sz = clauses.Count;
			SpanQuery[] newClauses = new SpanQuery[sz];
			
			for (int i = 0; i < sz; i++)
			{
				SpanQuery clause = clauses[i];
				newClauses[i] = (SpanQuery) clause.Clone();
			}
			SpanNearQuery spanNearQuery = new SpanNearQuery(newClauses, internalSlop, inOrder);
			spanNearQuery.Boost = Boost;
			return spanNearQuery;
		}
		
		/// <summary>Returns true iff <c>o</c> is equal to this. </summary>
		public  override bool Equals(System.Object o)
		{
			if (this == o)
				return true;
			if (!(o is SpanNearQuery))
				return false;
			
			SpanNearQuery spanNearQuery = (SpanNearQuery) o;
			
			if (inOrder != spanNearQuery.inOrder)
				return false;
			if (internalSlop != spanNearQuery.internalSlop)
				return false;
			if (clauses.Count != spanNearQuery.clauses.Count)
				return false;
            System.Collections.IEnumerator iter1 = clauses.GetEnumerator();
            System.Collections.IEnumerator iter2 = spanNearQuery.clauses.GetEnumerator();
            while (iter1.MoveNext() && iter2.MoveNext())
            {
                SpanQuery item1 = (SpanQuery)iter1.Current;
                SpanQuery item2 = (SpanQuery)iter2.Current;
                if (!item1.Equals(item2))
                    return false;
            }
			
			return Boost == spanNearQuery.Boost;
		}
		
		public override int GetHashCode()
		{
			long result = 0;
            //mgarski .NET uses the arraylist's location, not contents to calculate the hash
            // need to start with result being the hash of the contents.
            foreach (SpanQuery sq in clauses)
            {
                result += sq.GetHashCode();
            }
			// Mix bits before folding in things like boost, since it could cancel the
			// last element of clauses.  This particular mix also serves to
			// differentiate SpanNearQuery hashcodes from others.
			result ^= ((result << 14) | (Number.URShift(result, 19))); // reversible
			result += System.Convert.ToInt32(Boost);
			result += internalSlop;
			result ^= (inOrder ? (long) 0x99AFD3BD : 0);
			return (int) result;
		}
	}
}