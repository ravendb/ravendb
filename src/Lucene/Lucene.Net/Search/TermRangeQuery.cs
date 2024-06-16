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
using System.Globalization;
using Lucene.Net.Store;
using IndexReader = Lucene.Net.Index.IndexReader;
using ToStringUtils = Lucene.Net.Util.ToStringUtils;

namespace Lucene.Net.Search
{

    /// <summary> A Query that matches documents within an exclusive range of terms.
    /// 
    /// <p/>This query matches the documents looking for terms that fall into the
    /// supplied range according to <see cref="String.CompareTo(String)" />. It is not intended
    /// for numerical ranges, use <see cref="NumericRangeQuery{T}" /> instead.
    /// 
    /// <p/>This query uses the <see cref="MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT" />
    ///
    /// rewrite method.
    /// </summary>
    /// <since> 2.9
    /// </since>


        [Serializable]
    public class TermRangeQuery:MultiTermQuery
	{
		private System.String lowerTerm;
		private System.String upperTerm;
		private System.Globalization.CompareInfo collator;
		private System.String field;
		private bool includeLower;
		private bool includeUpper;
		
		
		/// <summary> Constructs a query selecting all terms greater/equal than <c>lowerTerm</c>
		/// but less/equal than <c>upperTerm</c>. 
		/// 
		/// <p/>
		/// If an endpoint is null, it is said 
		/// to be "open". Either or both endpoints may be open.  Open endpoints may not 
		/// be exclusive (you can't select all but the first or last term without 
		/// explicitly specifying the term to exclude.)
		/// 
		/// </summary>
		/// <param name="field">The field that holds both lower and upper terms.
		/// </param>
		/// <param name="lowerTerm">The term text at the lower end of the range
		/// </param>
		/// <param name="upperTerm">The term text at the upper end of the range
		/// </param>
		/// <param name="includeLower">If true, the <c>lowerTerm</c> is
		/// included in the range.
		/// </param>
		/// <param name="includeUpper">If true, the <c>upperTerm</c> is
		/// included in the range.
		/// </param>
		public TermRangeQuery(System.String field, System.String lowerTerm, System.String upperTerm, bool includeLower, bool includeUpper):this(field, lowerTerm, upperTerm, includeLower, includeUpper, null)
		{
		}

	    /// <summary>Constructs a query selecting all terms greater/equal than
	    /// <c>lowerTerm</c> but less/equal than <c>upperTerm</c>.
	    /// <p/>
	    /// If an endpoint is null, it is said 
	    /// to be "open". Either or both endpoints may be open.  Open endpoints may not 
	    /// be exclusive (you can't select all but the first or last term without 
	    /// explicitly specifying the term to exclude.)
	    /// <p/>
	    /// If <c>collator</c> is not null, it will be used to decide whether
	    /// index terms are within the given range, rather than using the Unicode code
	    /// point order in which index terms are stored.
	    /// <p/>
	    /// <strong>WARNING:</strong> Using this constructor and supplying a non-null
	    /// value in the <c>collator</c> parameter will cause every single 
	    /// index Term in the Field referenced by lowerTerm and/or upperTerm to be
	    /// examined.  Depending on the number of index Terms in this Field, the 
	    /// operation could be very slow.
	    /// 
	    /// </summary>
	    /// <param name="field"></param>
	    /// <param name="lowerTerm">The Term text at the lower end of the range
	    /// </param>
	    /// <param name="upperTerm">The Term text at the upper end of the range
	    /// </param>
	    /// <param name="includeLower">If true, the <c>lowerTerm</c> is
	    /// included in the range.
	    /// </param>
	    /// <param name="includeUpper">If true, the <c>upperTerm</c> is
	    /// included in the range.
	    /// </param>
	    /// <param name="collator">The collator to use to collate index Terms, to determine
	    /// their membership in the range bounded by <c>lowerTerm</c> and
	    /// <c>upperTerm</c>.
	    /// </param>
	    public TermRangeQuery(System.String field, System.String lowerTerm, System.String upperTerm, bool includeLower, bool includeUpper, System.Globalization.CompareInfo collator)
		{
			this.field = field;
			this.lowerTerm = lowerTerm;
			this.upperTerm = upperTerm;
			this.includeLower = includeLower;
			this.includeUpper = includeUpper;
			this.collator = collator;
		}

	    /// <summary>Returns the field name for this query </summary>
	    public virtual string Field
	    {
	        get { return field; }
	    }

	    /// <summary>Returns the lower value of this range query </summary>
	    public virtual string LowerTerm
	    {
	        get { return lowerTerm; }
	    }

	    /// <summary>Returns the upper value of this range query </summary>
	    public virtual string UpperTerm
	    {
	        get { return upperTerm; }
	    }

	    /// <summary>Returns <c>true</c> if the lower endpoint is inclusive </summary>
	    public virtual bool IncludesLower
	    {
	        get { return includeLower; }
	    }

	    /// <summary>Returns <c>true</c> if the upper endpoint is inclusive </summary>
	    public virtual bool IncludesUpper
	    {
	        get { return includeUpper; }
	    }

	    /// <summary>Returns the collator used to determine range inclusion, if any. </summary>
	    public virtual CompareInfo Collator
	    {
	        get { return collator; }
	    }

	    protected internal override FilteredTermEnum GetEnum(IndexReader reader, IState state)
		{
			return new TermRangeTermEnum(reader, field, lowerTerm, upperTerm, includeLower, includeUpper, collator, state);
		}
		
		/// <summary>Prints a user-readable version of this query. </summary>
		public override System.String ToString(System.String field)
		{
			System.Text.StringBuilder buffer = new System.Text.StringBuilder();
			if (!Field.Equals(field))
			{
				buffer.Append(Field);
				buffer.Append(":");
			}
			buffer.Append(includeLower?'[':'{');
			buffer.Append(lowerTerm != null?lowerTerm:"*");
			buffer.Append(" TO ");
			buffer.Append(upperTerm != null?upperTerm:"*");
			buffer.Append(includeUpper?']':'}');
			buffer.Append(ToStringUtils.Boost(Boost));
			return buffer.ToString();
		}
		
		//@Override
		public override int GetHashCode()
		{
			int prime = 31;
			int result = base.GetHashCode();
			result = prime * result + ((collator == null)?0:collator.GetHashCode());
			result = prime * result + ((field == null)?0:field.GetHashCode());
			result = prime * result + (includeLower?1231:1237);
			result = prime * result + (includeUpper?1231:1237);
			result = prime * result + ((lowerTerm == null)?0:lowerTerm.GetHashCode());
			result = prime * result + ((upperTerm == null)?0:upperTerm.GetHashCode());
			return result;
		}
		
		//@Override
		public  override bool Equals(System.Object obj)
		{
			if (this == obj)
				return true;
			if (!base.Equals(obj))
				return false;
			if (GetType() != obj.GetType())
				return false;
			TermRangeQuery other = (TermRangeQuery) obj;
			if (collator == null)
			{
				if (other.collator != null)
					return false;
			}
			else if (!collator.Equals(other.collator))
				return false;
			if (field == null)
			{
				if (other.field != null)
					return false;
			}
			else if (!field.Equals(other.field))
				return false;
			if (includeLower != other.includeLower)
				return false;
			if (includeUpper != other.includeUpper)
				return false;
			if (lowerTerm == null)
			{
				if (other.lowerTerm != null)
					return false;
			}
			else if (!lowerTerm.Equals(other.lowerTerm))
				return false;
			if (upperTerm == null)
			{
				if (other.upperTerm != null)
					return false;
			}
			else if (!upperTerm.Equals(other.upperTerm))
				return false;
			return true;
		}
	}
}