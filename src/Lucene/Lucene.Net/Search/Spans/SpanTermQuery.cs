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
using Lucene.Net.Store;
using IndexReader = Lucene.Net.Index.IndexReader;
using Term = Lucene.Net.Index.Term;
using ToStringUtils = Lucene.Net.Util.ToStringUtils;

namespace Lucene.Net.Search.Spans
{

    /// <summary>Matches spans containing a term. </summary>
    [Serializable]
    public class SpanTermQuery:SpanQuery
	{
		protected internal Term internalTerm;
		
		/// <summary>Construct a SpanTermQuery matching the named term's spans. </summary>
		public SpanTermQuery(Term term)
		{
			this.internalTerm = term;
		}

	    /// <summary>Return the term whose spans are matched. </summary>
	    public virtual Term Term
	    {
	        get { return internalTerm; }
	    }

	    public override string Field
	    {
	        get { return internalTerm.Field; }
	    }

	    public override void  ExtractTerms(System.Collections.Generic.ISet<Term> terms)
		{
		    terms.Add(internalTerm);
		}
		
		public override System.String ToString(System.String field)
		{
			System.Text.StringBuilder buffer = new System.Text.StringBuilder();
			if (internalTerm.Field.Equals(field))
				buffer.Append(internalTerm.Text);
			else
			{
				buffer.Append(internalTerm.ToString());
			}
			buffer.Append(ToStringUtils.Boost(Boost));
			return buffer.ToString();
		}
		
		public override int GetHashCode()
		{
			int prime = 31;
			int result = base.GetHashCode();
			result = prime * result + ((internalTerm == null)?0:internalTerm.GetHashCode());
			return result;
		}
		
		public  override bool Equals(System.Object obj)
		{
			if (this == obj)
				return true;
			if (!base.Equals(obj))
				return false;
			if (GetType() != obj.GetType())
				return false;
			SpanTermQuery other = (SpanTermQuery) obj;
			if (internalTerm == null)
			{
				if (other.internalTerm != null)
					return false;
			}
			else if (!internalTerm.Equals(other.internalTerm))
				return false;
			return true;
		}
		
		public override Spans GetSpans(IndexReader reader, IState state)
		{
			return new TermSpans(reader.TermPositions(internalTerm, state), internalTerm);
		}
	}
}