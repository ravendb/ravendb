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
using NumericTokenStream = Lucene.Net.Analysis.NumericTokenStream;
using NumericField = Lucene.Net.Documents.NumericField;
using IndexReader = Lucene.Net.Index.IndexReader;
using Term = Lucene.Net.Index.Term;
using NumericUtils = Lucene.Net.Util.NumericUtils;
using StringHelper = Lucene.Net.Util.StringHelper;
using ToStringUtils = Lucene.Net.Util.ToStringUtils;

namespace Lucene.Net.Search
{

    /// <summary> <p/>A <see cref="Query" /> that matches numeric values within a
    /// specified range.  To use this, you must first index the
    /// numeric values using <see cref="NumericField" /> (expert: <see cref="NumericTokenStream" />
    ///).  If your terms are instead textual,
    /// you should use <see cref="TermRangeQuery" />.  <see cref="NumericRangeFilter{T}" />
    /// is the filter equivalent of this
    /// query.<p/>
    /// 
    /// <p/>You create a new NumericRangeQuery with the static
    /// factory methods, eg:
    /// 
    /// <code>
    /// Query q = NumericRangeQuery.newFloatRange("weight",
    /// new Float(0.3f), new Float(0.10f),
    /// true, true);
    /// </code>
    /// 
    /// matches all documents whose float valued "weight" field
    /// ranges from 0.3 to 0.10, inclusive.
    /// 
    /// <p/>The performance of NumericRangeQuery is much better
    /// than the corresponding <see cref="TermRangeQuery" /> because the
    /// number of terms that must be searched is usually far
    /// fewer, thanks to trie indexing, described below.<p/>
    /// 
    /// <p/>You can optionally specify a <a
    /// href="#precisionStepDesc"><c>precisionStep</c></a>
    /// when creating this query.  This is necessary if you've
    /// changed this configuration from its default (4) during
    /// indexing.  Lower values consume more disk space but speed
    /// up searching.  Suitable values are between <b>1</b> and
    /// <b>8</b>. A good starting point to test is <b>4</b>,
    /// which is the default value for all <c>Numeric*</c>
    /// classes.  See <a href="#precisionStepDesc">below</a> for
    /// details.
    /// 
    /// <p/>This query defaults to
    /// <see cref="MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT"/> for
    /// 32 bit (int/float) ranges with precisionStep &lt;8 and 64
    /// bit (long/double) ranges with precisionStep &lt;6.
    /// Otherwise it uses 
    /// <see cref="MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE"/> as the
    /// number of terms is likely to be high.  With precision
    /// steps of &lt;4, this query can be run with one of the
    /// BooleanQuery rewrite methods without changing
    /// BooleanQuery's default max clause count.
    /// 
    /// <p/><font color="red"><b>NOTE:</b> This API is experimental and
    /// might change in incompatible ways in the next release.</font>
    /// 
    /// <br/><h3>How it works</h3>
    /// 
    /// <p/>See the publication about <a target="_blank" href="http://www.panfmp.org">panFMP</a>,
    /// where this algorithm was described (referred to as <c>TrieRangeQuery</c>):
    /// 
    /// <blockquote><strong>Schindler, U, Diepenbroek, M</strong>, 2008.
    /// <em>Generic XML-based Framework for Metadata Portals.</em>
    /// Computers &amp; Geosciences 34 (12), 1947-1955.
    /// <a href="http://dx.doi.org/10.1016/j.cageo.2008.02.023"
    /// target="_blank">doi:10.1016/j.cageo.2008.02.023</a></blockquote>
    /// 
    /// <p/><em>A quote from this paper:</em> Because Apache Lucene is a full-text
    /// search engine and not a conventional database, it cannot handle numerical ranges
    /// (e.g., field value is inside user defined bounds, even dates are numerical values).
    /// We have developed an extension to Apache Lucene that stores
    /// the numerical values in a special string-encoded format with variable precision
    /// (all numerical values like doubles, longs, floats, and ints are converted to
    /// lexicographic sortable string representations and stored with different precisions
    /// (for a more detailed description of how the values are stored,
    /// see <see cref="NumericUtils" />). A range is then divided recursively into multiple intervals for searching:
    /// The center of the range is searched only with the lowest possible precision in the <em>trie</em>,
    /// while the boundaries are matched more exactly. This reduces the number of terms dramatically.<p/>
    /// 
    /// <p/>For the variant that stores long values in 8 different precisions (each reduced by 8 bits) that
    /// uses a lowest precision of 1 byte, the index contains only a maximum of 256 distinct values in the
    /// lowest precision. Overall, a range could consist of a theoretical maximum of
    /// <c>7*255*2 + 255 = 3825</c> distinct terms (when there is a term for every distinct value of an
    /// 8-byte-number in the index and the range covers almost all of them; a maximum of 255 distinct values is used
    /// because it would always be possible to reduce the full 256 values to one term with degraded precision).
    /// In practice, we have seen up to 300 terms in most cases (index with 500,000 metadata records
    /// and a uniform value distribution).<p/>
    /// 
    /// <a name="precisionStepDesc"/><h3>Precision Step</h3>
    /// <p/>You can choose any <c>precisionStep</c> when encoding values.
    /// Lower step values mean more precisions and so more terms in index (and index gets larger).
    /// On the other hand, the maximum number of terms to match reduces, which optimized query speed.
    /// The formula to calculate the maximum term count is:
    /// <code>
    /// n = [ (bitsPerValue/precisionStep - 1) * (2^precisionStep - 1 ) * 2 ] + (2^precisionStep - 1 )
    /// </code>
    /// <p/><em>(this formula is only correct, when <c>bitsPerValue/precisionStep</c> is an integer;
    /// in other cases, the value must be rounded up and the last summand must contain the modulo of the division as
    /// precision step)</em>.
    /// For longs stored using a precision step of 4, <c>n = 15*15*2 + 15 = 465</c>, and for a precision
    /// step of 2, <c>n = 31*3*2 + 3 = 189</c>. But the faster search speed is reduced by more seeking
    /// in the term enum of the index. Because of this, the ideal <c>precisionStep</c> value can only
    /// be found out by testing. <b>Important:</b> You can index with a lower precision step value and test search speed
    /// using a multiple of the original step value.<p/>
    /// 
    /// <p/>Good values for <c>precisionStep</c> are depending on usage and data type:
    /// <list type="bullet">
    /// <item>The default for all data types is <b>4</b>, which is used, when no <c>precisionStep</c> is given.</item>
    /// <item>Ideal value in most cases for <em>64 bit</em> data types <em>(long, double)</em> is <b>6</b> or <b>8</b>.</item>
    /// <item>Ideal value in most cases for <em>32 bit</em> data types <em>(int, float)</em> is <b>4</b>.</item>
    /// <item>Steps <b>&gt;64</b> for <em>long/double</em> and <b>&gt;32</b> for <em>int/float</em> produces one token
    /// per value in the index and querying is as slow as a conventional <see cref="TermRangeQuery" />. But it can be used
    /// to produce fields, that are solely used for sorting (in this case simply use <see cref="int.MaxValue" /> as
    /// <c>precisionStep</c>). Using <see cref="NumericField">NumericFields</see> for sorting
    /// is ideal, because building the field cache is much faster than with text-only numbers.
    /// Sorting is also possible with range query optimized fields using one of the above <c>precisionSteps</c>.</item>
    /// </list>
    /// 
    /// <p/>Comparisons of the different types of RangeQueries on an index with about 500,000 docs showed
    /// that <see cref="TermRangeQuery" /> in boolean rewrite mode (with raised <see cref="BooleanQuery" /> clause count)
    /// took about 30-40 secs to complete, <see cref="TermRangeQuery" /> in constant score filter rewrite mode took 5 secs
    /// and executing this class took &lt;100ms to complete (on an Opteron64 machine, Java 1.5, 8 bit
    /// precision step). This query type was developed for a geographic portal, where the performance for
    /// e.g. bounding boxes or exact date/time stamps is important.<p/>
    /// 
    /// </summary>
    /// <since> 2.9
    /// 
    /// </since>

        [Serializable]
    public sealed class NumericRangeQuery<T> : MultiTermQuery
        where T : struct, IComparable<T> // best equiv constraint for java's number class
	{
		internal NumericRangeQuery(System.String field, int precisionStep, int valSize, T? min, T? max, bool minInclusive, bool maxInclusive)
		{
			System.Diagnostics.Debug.Assert((valSize == 32 || valSize == 64));
			if (precisionStep < 1)
				throw new System.ArgumentException("precisionStep must be >=1");
			this.field = StringHelper.Intern(field);
			this.precisionStep = precisionStep;
			this.valSize = valSize;
			this.min = min;
			this.max = max;
			this.minInclusive = minInclusive;
			this.maxInclusive = maxInclusive;
			
			// For bigger precisionSteps this query likely
			// hits too many terms, so set to CONSTANT_SCORE_FILTER right off
			// (especially as the FilteredTermEnum is costly if wasted only for AUTO tests because it
			// creates new enums from IndexReader for each sub-range)
			switch (valSize)
			{
				
				case 64: 
					RewriteMethod = (precisionStep > 6)?CONSTANT_SCORE_FILTER_REWRITE:CONSTANT_SCORE_AUTO_REWRITE_DEFAULT;
					break;
				
				case 32: 
					RewriteMethod = (precisionStep > 8)?CONSTANT_SCORE_FILTER_REWRITE:CONSTANT_SCORE_AUTO_REWRITE_DEFAULT;
					break;
				
				default: 
					// should never happen
					throw new System.ArgumentException("valSize must be 32 or 64");
				
			}
			
			// shortcut if upper bound == lower bound
			if (min != null && min.Equals(max))
			{
				RewriteMethod = CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE;
			}
		}
		
		//@Override
		protected internal override FilteredTermEnum GetEnum(IndexReader reader, IState state)
		{
			return new NumericRangeTermEnum(this, reader, state);
		}

	    /// <summary>Returns the field name for this query </summary>
	    public string Field
	    {
	        get { return field; }
	    }

	    /// <summary>Returns <c>true</c> if the lower endpoint is inclusive </summary>
	    public bool IncludesMin
	    {
	        get { return minInclusive; }
	    }

	    /// <summary>Returns <c>true</c> if the upper endpoint is inclusive </summary>
	    public bool IncludesMax
	    {
	        get { return maxInclusive; }
	    }

	    /// <summary>Returns the lower value of this range query </summary>
	    public T? Min
	    {
	        get { return min; }
	    }

	    /// <summary>Returns the upper value of this range query </summary>
	    public T? Max
	    {
	        get { return max; }
	    }

		public override System.String ToString(System.String field)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			if (!this.field.Equals(field))
				sb.Append(this.field).Append(':');
            return sb.Append(minInclusive ? '[' : '{').Append((min == null) ? "*" : min.ToString()).Append(" TO ").Append((max == null) ? "*" : max.ToString()).Append(maxInclusive ? ']' : '}').Append(ToStringUtils.Boost(Boost)).ToString();
        }
		
		public  override bool Equals(System.Object o)
		{
			if (o == this)
				return true;
			if (!base.Equals(o))
				return false;
			if (o is NumericRangeQuery<T>)
			{
                NumericRangeQuery<T> q = (NumericRangeQuery<T>)o;
                return ((System.Object)field == (System.Object)q.field && (q.min == null ? min == null : q.min.Equals(min)) && (q.max == null ? max == null : q.max.Equals(max)) && minInclusive == q.minInclusive && maxInclusive == q.maxInclusive && precisionStep == q.precisionStep);
            }
			return false;
		}
		
		public override int GetHashCode()
		{
			int hash = base.GetHashCode();
            hash += (field.GetHashCode() ^ 0x4565fd66 + precisionStep ^ 0x64365465);
            if (min != null)
                hash += (min.GetHashCode() ^ 0x14fa55fb);
            if (max != null)
                hash += (max.GetHashCode() ^ 0x733fa5fe);
			return hash + (minInclusive.GetHashCode() ^ 0x14fa55fb) + (maxInclusive.GetHashCode() ^ 0x733fa5fe);
		}

         // field must be interned after reading from stream
        //private void ReadObject(java.io.ObjectInputStream in) 
        //{
        //    in.defaultReadObject();
        //    field = StringHelper.intern(field);
        //}


        [System.Runtime.Serialization.OnDeserialized]
        internal void OnDeserialized(System.Runtime.Serialization.StreamingContext context)
        {
            field = StringHelper.Intern(field);
        }
		
		// members (package private, to be also fast accessible by NumericRangeTermEnum)
		internal System.String field;
		internal int precisionStep;
		internal int valSize;
		internal T? min;
		internal T? max;
		internal bool minInclusive;
		internal bool maxInclusive;
		
		/// <summary> Subclass of FilteredTermEnum for enumerating all terms that match the
		/// sub-ranges for trie range queries.
		/// <p/>
		/// WARNING: This term enumeration is not guaranteed to be always ordered by
		/// <see cref="Term.CompareTo(Term)" />.
		/// The ordering depends on how <see cref="NumericUtils.SplitLongRange" /> and
		/// <see cref="NumericUtils.SplitIntRange" /> generates the sub-ranges. For
		/// <see cref="MultiTermQuery" /> ordering is not relevant.
		/// </summary>
		private sealed class NumericRangeTermEnum:FilteredTermEnum
		{
			private class AnonymousClassLongRangeBuilder:NumericUtils.LongRangeBuilder
			{
				public AnonymousClassLongRangeBuilder(NumericRangeTermEnum enclosingInstance)
				{
					InitBlock(enclosingInstance);
				}
				private void  InitBlock(NumericRangeTermEnum enclosingInstance)
				{
					this.enclosingInstance = enclosingInstance;
				}
				private NumericRangeTermEnum enclosingInstance;
				public NumericRangeTermEnum Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				//@Override
				public override void  AddRange(System.String minPrefixCoded, System.String maxPrefixCoded)
				{
					Enclosing_Instance.rangeBounds.AddLast(minPrefixCoded);
                    Enclosing_Instance.rangeBounds.AddLast(maxPrefixCoded);
				}
			}
			private class AnonymousClassIntRangeBuilder:NumericUtils.IntRangeBuilder
			{
				public AnonymousClassIntRangeBuilder(NumericRangeTermEnum enclosingInstance)
				{
					InitBlock(enclosingInstance);
				}
				private void  InitBlock(NumericRangeTermEnum enclosingInstance)
				{
					this.enclosingInstance = enclosingInstance;
				}
				private NumericRangeTermEnum enclosingInstance;
				public NumericRangeTermEnum Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				//@Override
				public override void  AddRange(System.String minPrefixCoded, System.String maxPrefixCoded)
				{
                    Enclosing_Instance.rangeBounds.AddLast(minPrefixCoded);
                    Enclosing_Instance.rangeBounds.AddLast(maxPrefixCoded);
				}
			}
			private void  InitBlock(NumericRangeQuery<T> enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
                termTemplate = new Term(Enclosing_Instance.field);
			}
            private NumericRangeQuery<T> enclosingInstance;
            public NumericRangeQuery<T> Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			private IndexReader reader;
            private LinkedList<string> rangeBounds = new LinkedList<string>();
		    private Term termTemplate;
			private System.String currentUpperBound = null;

		    private bool isDisposed;

            internal NumericRangeTermEnum(NumericRangeQuery<T> enclosingInstance, IndexReader reader, IState state)
			{
				InitBlock(enclosingInstance);
				this.reader = reader;
				
				Type rangeType = Nullable.GetUnderlyingType(typeof(T?));
				switch (Enclosing_Instance.valSize)
				{
					case 64:  {
							// lower
							long minBound = System.Int64.MinValue;
                            if (rangeType == typeof(System.Int64))
                            {
                                // added in these checks to emulate java.  passing null give it no type (in old code), 
                                // but .net can identifies it with generics and sets the bounds to 0, causing tests to fail
                                if (Enclosing_Instance.min != null) 
								    minBound = System.Convert.ToInt64(Enclosing_Instance.min);
							}
                            else if (rangeType == typeof(System.Double))
                            {
                                if (Enclosing_Instance.min != null)
								    minBound = NumericUtils.DoubleToSortableLong(System.Convert.ToDouble(Enclosing_Instance.min));
							}
                            if (!Enclosing_Instance.minInclusive && Enclosing_Instance.min != null)
							{
								if (minBound == System.Int64.MaxValue)
									break;
								minBound++;
							}
							
							// upper
							long maxBound = System.Int64.MaxValue;
                            if (rangeType == typeof(System.Int64))
                            {
                                if (Enclosing_Instance.max != null)
								    maxBound = System.Convert.ToInt64(Enclosing_Instance.max);
							}
                            else if (rangeType == typeof(System.Double))
                            {
                                if (Enclosing_Instance.max != null)
								    maxBound = NumericUtils.DoubleToSortableLong(System.Convert.ToDouble(Enclosing_Instance.max));
							}
                            if (!Enclosing_Instance.maxInclusive && Enclosing_Instance.max != null)
							{
								if (maxBound == System.Int64.MinValue)
									break;
								maxBound--;
							}
							
							NumericUtils.SplitLongRange(new AnonymousClassLongRangeBuilder(this), Enclosing_Instance.precisionStep, minBound, maxBound);
							break;
						}
					
					
					case 32:  {
							// lower
							int minBound = System.Int32.MinValue;
                            if (rangeType == typeof(System.Int32))
							{
                                if (Enclosing_Instance.min != null)
								    minBound = System.Convert.ToInt32(Enclosing_Instance.min);
                            }
                            else if (rangeType == typeof(System.Single))
                            {
                                if (Enclosing_Instance.min != null)
								    minBound = NumericUtils.FloatToSortableInt(System.Convert.ToSingle(Enclosing_Instance.min));
							}
                            if (!Enclosing_Instance.minInclusive && Enclosing_Instance.min != null)
							{
								if (minBound == System.Int32.MaxValue)
									break;
								minBound++;
							}
							
							// upper
                            int maxBound = System.Int32.MaxValue;
                            if (rangeType == typeof(System.Int32))
                            {
                                if (Enclosing_Instance.max != null)
								    maxBound = System.Convert.ToInt32(Enclosing_Instance.max);
							}
                            else if (rangeType == typeof(System.Single))
                            {
                                if (Enclosing_Instance.max != null)
								    maxBound = NumericUtils.FloatToSortableInt(System.Convert.ToSingle(Enclosing_Instance.max));
							}
                            if (!Enclosing_Instance.maxInclusive && Enclosing_Instance.max != null)
							{
								if (maxBound == System.Int32.MinValue)
									break;
								maxBound--;
							}
							
							NumericUtils.SplitIntRange(new AnonymousClassIntRangeBuilder(this), Enclosing_Instance.precisionStep, minBound, maxBound);
							break;
						}
					
					
					default: 
						// should never happen
						throw new System.ArgumentException("valSize must be 32 or 64");
					
				}
				
				// seek to first term
				Next(state);
			}
			
			//@Override
			public override float Difference()
			{
				return 1.0f;
			}
			
			/// <summary>this is a dummy, it is not used by this class. </summary>
			//@Override
			public override bool EndEnum()
			{
			    throw new NotSupportedException("not implemented");
			}

            /// <summary>this is a dummy, it is not used by this class. </summary>
            protected internal override void SetEnum(TermEnum tenum, IState state)
            {
                throw new NotSupportedException("not implemented");
            }
			
			/// <summary> Compares if current upper bound is reached,
			/// this also updates the term count for statistics.
			/// In contrast to <see cref="FilteredTermEnum" />, a return value
			/// of <c>false</c> ends iterating the current enum
			/// and forwards to the next sub-range.
			/// </summary>
			//@Override
			protected internal override bool TermCompare(Term term)
			{
				return (term.Field == Enclosing_Instance.field && String.CompareOrdinal(term.Text, currentUpperBound) <= 0);
			}
			
			/// <summary>Increments the enumeration to the next element.  True if one exists. </summary>
			//@Override
            public override bool Next(IState state)
			{
			    // if a current term exists, the actual enum is initialized:
			    // try change to next term, if no such term exists, fall-through
			    if (currentTerm != null)
			    {
			        System.Diagnostics.Debug.Assert(actualEnum != null);
			        if (actualEnum.Next(state))
			        {
			            currentTerm = actualEnum.Term;
			            if (TermCompare(currentTerm))
			                return true;
			        }
			    }
			    // if all above fails, we go forward to the next enum,
			    // if one is available
                currentTerm = null;
			    while (rangeBounds.Count >= 2)
			    {
			        // close the current enum and read next bounds
			        if (actualEnum != null)
			        {
			            actualEnum.Close();
			            actualEnum = null;
			        }
			        string lowerBound = rangeBounds.First.Value;
			        rangeBounds.RemoveFirst();
			        this.currentUpperBound = rangeBounds.First.Value;
			        rangeBounds.RemoveFirst();
			        // create a new enum
			        actualEnum = reader.Terms(termTemplate.CreateTerm(lowerBound), state);
			        currentTerm = actualEnum.Term;
			        if (currentTerm != null && TermCompare(currentTerm))
			            return true;
			        // clear the current term for next iteration
                    currentTerm = null;
			    }

			    // no more sub-range enums available
			    System.Diagnostics.Debug.Assert(rangeBounds.Count == 0 && currentTerm == null);
			    return false;
			}

		    /// <summary>Closes the enumeration to further activity, freeing resources.  </summary>
            protected override void Dispose(bool disposing)
            {
                if (isDisposed) return;

                rangeBounds.Clear();
                currentUpperBound = null;

		        isDisposed = true;
                base.Dispose(disposing);
            }
		}
	}

    public static class NumericRangeQuery
    {
        /// <summary> Factory that creates a <c>NumericRangeQuery</c>, that queries a <c>long</c>
        /// range using the given <a href="#precisionStepDesc"><c>precisionStep</c></a>.
        /// You can have half-open ranges (which are in fact &lt;/&#8804; or &gt;/&#8805; queries)
        /// by setting the min or max value to <c>null</c>. By setting inclusive to false, it will
        /// match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
        /// </summary>
        public static NumericRangeQuery<long> NewLongRange(System.String field, int precisionStep, long? min, long? max, bool minInclusive, bool maxInclusive)
        {
            return new NumericRangeQuery<long>(field, precisionStep, 64, min, max, minInclusive, maxInclusive);
        }

        /// <summary> Factory that creates a <c>NumericRangeQuery</c>, that queries a <c>long</c>
        /// range using the default <c>precisionStep</c> <see cref="NumericUtils.PRECISION_STEP_DEFAULT" /> (4).
        /// You can have half-open ranges (which are in fact &lt;/&#8804; or &gt;/&#8805; queries)
        /// by setting the min or max value to <c>null</c>. By setting inclusive to false, it will
        /// match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
        /// </summary>
        public static NumericRangeQuery<long> NewLongRange(System.String field, long? min, long? max, bool minInclusive, bool maxInclusive)
        {
            return new NumericRangeQuery<long>(field, NumericUtils.PRECISION_STEP_DEFAULT, 64, min, max, minInclusive, maxInclusive);
        }

        /// <summary> Factory that creates a <c>NumericRangeQuery</c>, that queries a <c>int</c>
        /// range using the given <a href="#precisionStepDesc"><c>precisionStep</c></a>.
        /// You can have half-open ranges (which are in fact &lt;/&#8804; or &gt;/&#8805; queries)
        /// by setting the min or max value to <c>null</c>. By setting inclusive to false, it will
        /// match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
        /// </summary>
        public static NumericRangeQuery<int> NewIntRange(System.String field, int precisionStep, int? min, int? max, bool minInclusive, bool maxInclusive)
        {
            return new NumericRangeQuery<int>(field, precisionStep, 32, min, max, minInclusive, maxInclusive);
        }

        /// <summary> Factory that creates a <c>NumericRangeQuery</c>, that queries a <c>int</c>
        /// range using the default <c>precisionStep</c> <see cref="NumericUtils.PRECISION_STEP_DEFAULT" /> (4).
        /// You can have half-open ranges (which are in fact &lt;/&#8804; or &gt;/&#8805; queries)
        /// by setting the min or max value to <c>null</c>. By setting inclusive to false, it will
        /// match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
        /// </summary>
        public static NumericRangeQuery<int> NewIntRange(System.String field, int? min, int? max, bool minInclusive, bool maxInclusive)
        {
            return new NumericRangeQuery<int>(field, NumericUtils.PRECISION_STEP_DEFAULT, 32, min, max, minInclusive, maxInclusive);
        }

        /// <summary> Factory that creates a <c>NumericRangeQuery</c>, that queries a <c>double</c>
        /// range using the given <a href="#precisionStepDesc"><c>precisionStep</c></a>.
        /// You can have half-open ranges (which are in fact &lt;/&#8804; or &gt;/&#8805; queries)
        /// by setting the min or max value to <c>null</c>. By setting inclusive to false, it will
        /// match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
        /// </summary>
        public static NumericRangeQuery<double> NewDoubleRange(System.String field, int precisionStep, double? min, double? max, bool minInclusive, bool maxInclusive)
        {
            return new NumericRangeQuery<double>(field, precisionStep, 64, min, max, minInclusive, maxInclusive);
        }

        /// <summary> Factory that creates a <c>NumericRangeQuery</c>, that queries a <c>double</c>
        /// range using the default <c>precisionStep</c> <see cref="NumericUtils.PRECISION_STEP_DEFAULT" /> (4).
        /// You can have half-open ranges (which are in fact &lt;/&#8804; or &gt;/&#8805; queries)
        /// by setting the min or max value to <c>null</c>. By setting inclusive to false, it will
        /// match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
        /// </summary>
        public static NumericRangeQuery<double> NewDoubleRange(System.String field, double? min, double? max, bool minInclusive, bool maxInclusive)
        {
            return new NumericRangeQuery<double>(field, NumericUtils.PRECISION_STEP_DEFAULT, 64, min, max, minInclusive, maxInclusive);
        }

        /// <summary> Factory that creates a <c>NumericRangeQuery</c>, that queries a <c>float</c>
        /// range using the given <a href="#precisionStepDesc"><c>precisionStep</c></a>.
        /// You can have half-open ranges (which are in fact &lt;/&#8804; or &gt;/&#8805; queries)
        /// by setting the min or max value to <c>null</c>. By setting inclusive to false, it will
        /// match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
        /// </summary>
        public static NumericRangeQuery<float> NewFloatRange(System.String field, int precisionStep, float? min, float? max, bool minInclusive, bool maxInclusive)
        {
            return new NumericRangeQuery<float>(field, precisionStep, 32, min, max, minInclusive, maxInclusive);
        }

        /// <summary> Factory that creates a <c>NumericRangeQuery</c>, that queries a <c>float</c>
        /// range using the default <c>precisionStep</c> <see cref="NumericUtils.PRECISION_STEP_DEFAULT" /> (4).
        /// You can have half-open ranges (which are in fact &lt;/&#8804; or &gt;/&#8805; queries)
        /// by setting the min or max value to <c>null</c>. By setting inclusive to false, it will
        /// match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
        /// </summary>
        public static NumericRangeQuery<float> NewFloatRange(System.String field, float? min, float? max, bool minInclusive, bool maxInclusive)
        {
            return new NumericRangeQuery<float>(field, NumericUtils.PRECISION_STEP_DEFAULT, 32, min, max, minInclusive, maxInclusive);
        }
    }
}