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

using NumericTokenStream = Lucene.Net.Analysis.NumericTokenStream;
using NumericField = Lucene.Net.Documents.NumericField;
using NumericUtils = Lucene.Net.Util.NumericUtils;

namespace Lucene.Net.Search
{

    /// <summary> A <see cref="Filter" /> that only accepts numeric values within
    /// a specified range. To use this, you must first index the
    /// numeric values using <see cref="NumericField" /> (expert: <see cref="NumericTokenStream" />
    ///).
    /// 
    /// <p/>You create a new NumericRangeFilter with the static
    /// factory methods, eg:
    /// 
    /// <code>
    /// Filter f = NumericRangeFilter.newFloatRange("weight",
    ///             new Float(0.3f), new Float(0.10f),
    ///             true, true);
    /// </code>
    /// 
    /// accepts all documents whose float valued "weight" field
    /// ranges from 0.3 to 0.10, inclusive.
    /// See <see cref="NumericRangeQuery{T}" /> for details on how Lucene
    /// indexes and searches numeric valued fields.
    /// 
    /// <p/><font color="red"><b>NOTE:</b> This API is experimental and
    /// might change in incompatible ways in the next
    /// release.</font>
    /// 
    /// </summary>
    /// <since> 2.9
    /// 
    /// </since>

        [Serializable]
    public sealed class NumericRangeFilter<T> : MultiTermQueryWrapperFilter<NumericRangeQuery<T>>
        where T : struct, IComparable<T>
        // real numbers in C# are structs and IComparable with themselves, best constraint we have
    {
        internal NumericRangeFilter(NumericRangeQuery<T> query)
            : base(query)
        {
        }

        /// <summary>Returns the field name for this filter </summary>
        public string Field
        {
            get { return query.Field; }
        }

        /// <summary>Returns <c>true</c> if the lower endpoint is inclusive </summary>
        public bool IncludesMin
        {
            get { return query.IncludesMin; }
        }

        /// <summary>Returns <c>true</c> if the upper endpoint is inclusive </summary>
        public bool IncludesMax
        {
            get { return query.IncludesMax; }
        }

        /// <summary>Returns the lower value of this range filter </summary>
        public T? Min
        {
            get { return query.Min; }
        }

        /// <summary>Returns the upper value of this range filter </summary>
        public T? Max
        {
            get { return query.Max; }
        }
    }

    public static class NumericRangeFilter
    {
        /// <summary> Factory that creates a <c>NumericRangeFilter</c>, that filters a <c>long</c>
        /// range using the given <a href="NumericRangeQuery.html#precisionStepDesc"><c>precisionStep</c></a>.
        /// You can have half-open ranges (which are in fact &lt;/&#8804; or &gt;/&#8805; queries)
        /// by setting the min or max value to <c>null</c>. By setting inclusive to false, it will
        /// match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
        /// </summary>
        public static NumericRangeFilter<long> NewLongRange(System.String field, int precisionStep, long? min, long? max, bool minInclusive, bool maxInclusive)
        {
            return new NumericRangeFilter<long>(NumericRangeQuery.NewLongRange(field, precisionStep, min, max, minInclusive, maxInclusive));
        }

        /// <summary> Factory that creates a <c>NumericRangeFilter</c>, that queries a <c>long</c>
        /// range using the default <c>precisionStep</c> <see cref="NumericUtils.PRECISION_STEP_DEFAULT" /> (4).
        /// You can have half-open ranges (which are in fact &lt;/&#8804; or &gt;/&#8805; queries)
        /// by setting the min or max value to <c>null</c>. By setting inclusive to false, it will
        /// match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
        /// </summary>
        public static NumericRangeFilter<long> NewLongRange(System.String field, long? min, long? max, bool minInclusive, bool maxInclusive)
        {
            return new NumericRangeFilter<long>(NumericRangeQuery.NewLongRange(field, min, max, minInclusive, maxInclusive));
        }

        /// <summary> Factory that creates a <c>NumericRangeFilter</c>, that filters a <c>int</c>
        /// range using the given <a href="NumericRangeQuery.html#precisionStepDesc"><c>precisionStep</c></a>.
        /// You can have half-open ranges (which are in fact &lt;/&#8804; or &gt;/&#8805; queries)
        /// by setting the min or max value to <c>null</c>. By setting inclusive to false, it will
        /// match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
        /// </summary>
        public static NumericRangeFilter<int> NewIntRange(System.String field, int precisionStep, int? min, int? max, bool minInclusive, bool maxInclusive)
        {
            return new NumericRangeFilter<int>(NumericRangeQuery.NewIntRange(field, precisionStep, min, max, minInclusive, maxInclusive));
        }

        /// <summary> Factory that creates a <c>NumericRangeFilter</c>, that queries a <c>int</c>
        /// range using the default <c>precisionStep</c> <see cref="NumericUtils.PRECISION_STEP_DEFAULT" /> (4).
        /// You can have half-open ranges (which are in fact &lt;/&#8804; or &gt;/&#8805; queries)
        /// by setting the min or max value to <c>null</c>. By setting inclusive to false, it will
        /// match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
        /// </summary>
        public static NumericRangeFilter<int> NewIntRange(System.String field, int? min, int? max, bool minInclusive, bool maxInclusive)
        {
            return new NumericRangeFilter<int>(NumericRangeQuery.NewIntRange(field, min, max, minInclusive, maxInclusive));
        }

        /// <summary> Factory that creates a <c>NumericRangeFilter</c>, that filters a <c>double</c>
        /// range using the given <a href="NumericRangeQuery.html#precisionStepDesc"><c>precisionStep</c></a>.
        /// You can have half-open ranges (which are in fact &lt;/&#8804; or &gt;/&#8805; queries)
        /// by setting the min or max value to <c>null</c>. By setting inclusive to false, it will
        /// match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
        /// </summary>
        public static NumericRangeFilter<double> NewDoubleRange(System.String field, int precisionStep, double? min, double? max, bool minInclusive, bool maxInclusive)
        {
            return new NumericRangeFilter<double>(NumericRangeQuery.NewDoubleRange(field, precisionStep, min, max, minInclusive, maxInclusive));
        }

        /// <summary> Factory that creates a <c>NumericRangeFilter</c>, that queries a <c>double</c>
        /// range using the default <c>precisionStep</c> <see cref="NumericUtils.PRECISION_STEP_DEFAULT" /> (4).
        /// You can have half-open ranges (which are in fact &lt;/&#8804; or &gt;/&#8805; queries)
        /// by setting the min or max value to <c>null</c>. By setting inclusive to false, it will
        /// match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
        /// </summary>
        public static NumericRangeFilter<double> NewDoubleRange(System.String field, double? min, double? max, bool minInclusive, bool maxInclusive)
        {
            return new NumericRangeFilter<double>(NumericRangeQuery.NewDoubleRange(field, min, max, minInclusive, maxInclusive));
        }

        /// <summary> Factory that creates a <c>NumericRangeFilter</c>, that filters a <c>float</c>
        /// range using the given <a href="NumericRangeQuery.html#precisionStepDesc"><c>precisionStep</c></a>.
        /// You can have half-open ranges (which are in fact &lt;/&#8804; or &gt;/&#8805; queries)
        /// by setting the min or max value to <c>null</c>. By setting inclusive to false, it will
        /// match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
        /// </summary>
        public static NumericRangeFilter<float> NewFloatRange(System.String field, int precisionStep, float? min, float? max, bool minInclusive, bool maxInclusive)
        {
            return new NumericRangeFilter<float>(NumericRangeQuery.NewFloatRange(field, precisionStep, min, max, minInclusive, maxInclusive));
        }

        /// <summary> Factory that creates a <c>NumericRangeFilter</c>, that queries a <c>float</c>
        /// range using the default <c>precisionStep</c> <see cref="NumericUtils.PRECISION_STEP_DEFAULT" /> (4).
        /// You can have half-open ranges (which are in fact &lt;/&#8804; or &gt;/&#8805; queries)
        /// by setting the min or max value to <c>null</c>. By setting inclusive to false, it will
        /// match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
        /// </summary>
        public static NumericRangeFilter<float> NewFloatRange(System.String field, float? min, float? max, bool minInclusive, bool maxInclusive)
        {
            return new NumericRangeFilter<float>(NumericRangeQuery.NewFloatRange(field, min, max, minInclusive, maxInclusive));
        }
    }
}