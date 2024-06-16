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
using Lucene.Net.Util;
using IndexReader = Lucene.Net.Index.IndexReader;
using ToStringUtils = Lucene.Net.Util.ToStringUtils;
using Query = Lucene.Net.Search.Query;

namespace Lucene.Net.Search.Spans
{

    /// <summary>Matches the union of its clauses.</summary>
    [Serializable]
    public class SpanOrQuery : SpanQuery, System.ICloneable
	{
		private class AnonymousClassSpans : Spans
		{
			public AnonymousClassSpans(Lucene.Net.Index.IndexReader reader, SpanOrQuery enclosingInstance)
			{
				InitBlock(reader, enclosingInstance);
			}
			private void  InitBlock(Lucene.Net.Index.IndexReader reader, SpanOrQuery enclosingInstance)
			{
				this.reader = reader;
				this.enclosingInstance = enclosingInstance;
			}
			private Lucene.Net.Index.IndexReader reader;
			private SpanOrQuery enclosingInstance;
			public SpanOrQuery Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private SpanQueue queue = null;
			
			private bool InitSpanQueue(int target, IState state)
			{
				queue = new SpanQueue(enclosingInstance, Enclosing_Instance.clauses.Count);
				System.Collections.Generic.IEnumerator<SpanQuery> i = Enclosing_Instance.clauses.GetEnumerator();
				while (i.MoveNext())
				{
					Spans spans = i.Current.GetSpans(reader, state);
					if (((target == - 1) && spans.Next(state)) || ((target != - 1) && spans.SkipTo(target, state)))
					{
						queue.Add(spans);
					}
				}
				return queue.Size() != 0;
			}
			
			public override bool Next(IState state)
			{
				if (queue == null)
				{
					return InitSpanQueue(- 1, state);
				}
				
				if (queue.Size() == 0)
				{
					// all done
					return false;
				}
				
				if (Top().Next(state))
				{
					// move to next
					queue.UpdateTop();
					return true;
				}
				
				queue.Pop(); // exhausted a clause
				return queue.Size() != 0;
			}
			
			private Spans Top()
			{
				return queue.Top();
			}
			
			public override bool SkipTo(int target, IState state)
			{
				if (queue == null)
				{
					return InitSpanQueue(target, state);
				}
				
				bool skipCalled = false;
				while (queue.Size() != 0 && Top().Doc() < target)
				{
					if (Top().SkipTo(target, state))
					{
						queue.UpdateTop();
					}
					else
					{
						queue.Pop();
					}
					skipCalled = true;
				}
				
				if (skipCalled)
				{
					return queue.Size() != 0;
				}
				return Next(state);
			}
			
			public override int Doc()
			{
				return Top().Doc();
			}
			public override int Start()
			{
				return Top().Start();
			}
			public override int End()
			{
				return Top().End();
			}

		    public override ICollection<byte[]> GetPayload(IState state)
		    {
		        System.Collections.Generic.ICollection<byte[]> result = null;
		        Spans theTop = Top();
		        if (theTop != null && theTop.IsPayloadAvailable())
		        {
		            result = theTop.GetPayload(state);
		        }
		        return result;
		    }

		    public override bool IsPayloadAvailable()
		    {
		        Spans top = Top();
		        return top != null && top.IsPayloadAvailable();
		    }

		    public override System.String ToString()
			{
				return "spans(" + Enclosing_Instance + ")@" + ((queue == null)?"START":(queue.Size() > 0?(Doc() + ":" + Start() + "-" + End()):"END"));
			}
		}

		private EquatableList<SpanQuery> clauses;
		private System.String field;
		
		/// <summary>Construct a SpanOrQuery merging the provided clauses. </summary>
		public SpanOrQuery(params SpanQuery[] clauses)
		{
			
			// copy clauses array into an ArrayList
			this.clauses = new EquatableList<SpanQuery>(clauses.Length);
			for (int i = 0; i < clauses.Length; i++)
			{
				SpanQuery clause = clauses[i];
				if (i == 0)
				{
					// check field
					field = clause.Field;
				}
				else if (!clause.Field.Equals(field))
				{
					throw new System.ArgumentException("Clauses must have same field.");
				}
				this.clauses.Add(clause);
			}
		}
		
		/// <summary>Return the clauses whose spans are matched. </summary>
		public virtual SpanQuery[] GetClauses()
		{
			return clauses.ToArray();
		}

	    public override string Field
	    {
	        get { return field; }
	    }

	    public override void  ExtractTerms(System.Collections.Generic.ISet<Term> terms)
		{
			foreach(SpanQuery clause in clauses)
            {
				clause.ExtractTerms(terms);
			}
		}
		
		public override System.Object Clone()
		{
			int sz = clauses.Count;
			SpanQuery[] newClauses = new SpanQuery[sz];
			
			for (int i = 0; i < sz; i++)
			{
                newClauses[i] = (SpanQuery) clauses[i].Clone();
			}
			SpanOrQuery soq = new SpanOrQuery(newClauses);
			soq.Boost = Boost;
			return soq;
		}
		
		public override Query Rewrite(IndexReader reader, IState state)
		{
			SpanOrQuery clone = null;
			for (int i = 0; i < clauses.Count; i++)
			{
				SpanQuery c = clauses[i];
				SpanQuery query = (SpanQuery) c.Rewrite(reader, state);
				if (query != c)
				{
					// clause rewrote: must clone
					if (clone == null)
						clone = (SpanOrQuery) this.Clone();
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
		
		public override System.String ToString(System.String field)
		{
			System.Text.StringBuilder buffer = new System.Text.StringBuilder();
			buffer.Append("spanOr([");
			System.Collections.Generic.IEnumerator<SpanQuery> i = clauses.GetEnumerator();
            int j = 0;
			while (i.MoveNext())
			{
                j++;
				SpanQuery clause = i.Current;
				buffer.Append(clause.ToString(field));
                if (j < clauses.Count)
                {
                    buffer.Append(", ");
                }
			}
			buffer.Append("])");
			buffer.Append(ToStringUtils.Boost(Boost));
			return buffer.ToString();
		}
		
		public  override bool Equals(System.Object o)
		{
			if (this == o)
				return true;
			if (o == null || GetType() != o.GetType())
				return false;
			
			SpanOrQuery that = (SpanOrQuery) o;
			
			if (!clauses.Equals(that.clauses))
				return false;
			if (!(clauses.Count == 0) && !field.Equals(that.field))
				return false;
			
			return Boost == that.Boost;
		}
		
		public override int GetHashCode()
		{
			int h = clauses.GetHashCode();
			h ^= ((h << 10) | (Number.URShift(h, 23)));
			h ^= System.Convert.ToInt32(Boost);
			return h;
		}
		
		
		private class SpanQueue : PriorityQueue<Spans>
		{
			private void  InitBlock(SpanOrQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private SpanOrQuery enclosingInstance;
			public SpanOrQuery Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public SpanQueue(SpanOrQuery enclosingInstance, int size)
			{
				InitBlock(enclosingInstance);
				Initialize(size);
			}

            public override bool LessThan(Spans spans1, Spans spans2)
			{
				if (spans1.Doc() == spans2.Doc())
				{
					if (spans1.Start() == spans2.Start())
					{
						return spans1.End() < spans2.End();
					}
					else
					{
						return spans1.Start() < spans2.Start();
					}
				}
				else
				{
					return spans1.Doc() < spans2.Doc();
				}
			}
		}
		
		public override Spans GetSpans(IndexReader reader, IState state)
		{
			if (clauses.Count == 1)
			// optimize 1-clause case
				return (clauses[0]).GetSpans(reader, state);
			
			return new AnonymousClassSpans(reader, this);
		}
	}
}