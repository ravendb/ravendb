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
using Lucene.Net.Index;
using Lucene.Net.Store;
using Lucene.Net.Support;
using NumericField = Lucene.Net.Documents.NumericField;
using IndexReader = Lucene.Net.Index.IndexReader;
using TermDocs = Lucene.Net.Index.TermDocs;
using NumericUtils = Lucene.Net.Util.NumericUtils;

namespace Lucene.Net.Search
{
	
	/// <summary> A range filter built on top of a cached single term field (in <see cref="FieldCache" />).
	/// 
    /// <p/><see cref="FieldCacheRangeFilter" /> builds a single cache for the field the first time it is used.
    /// Each subsequent <see cref="FieldCacheRangeFilter" /> on the same field then reuses this cache,
	/// even if the range itself changes. 
	/// 
    /// <p/>This means that <see cref="FieldCacheRangeFilter" /> is much faster (sometimes more than 100x as fast) 
	/// as building a <see cref="TermRangeFilter" /> if using a <see cref="NewStringRange" />. However, if the range never changes it
    /// is slower (around 2x as slow) than building a CachingWrapperFilter on top of a single <see cref="TermRangeFilter" />.
	/// 
	/// For numeric data types, this filter may be significantly faster than <see cref="NumericRangeFilter{T}" />.
	/// Furthermore, it does not need the numeric values encoded by <see cref="NumericField" />. But
	/// it has the problem that it only works with exact one value/document (see below).
	/// 
    /// <p/>As with all <see cref="FieldCache" /> based functionality, <see cref="FieldCacheRangeFilter" /> is only valid for 
	/// fields which exact one term for each document (except for <see cref="NewStringRange" />
	/// where 0 terms are also allowed). Due to a restriction of <see cref="FieldCache" />, for numeric ranges
	/// all terms that do not have a numeric value, 0 is assumed.
	/// 
	/// <p/>Thus it works on dates, prices and other single value fields but will not work on
	/// regular text fields. It is preferable to use a <c>NOT_ANALYZED</c> field to ensure that
	/// there is only a single term. 
	/// 
	/// <p/>This class does not have an constructor, use one of the static factory methods available,
	/// that create a correct instance for different data types supported by <see cref="FieldCache" />.
	/// </summary>
	
    public static class FieldCacheRangeFilter
	{

        [Serializable]

        private class AnonymousClassFieldCacheRangeFilter : FieldCacheRangeFilter<string>
        {
            private class AnonymousClassFieldCacheDocIdSet : FieldCacheDocIdSet
            {
                private void InitBlock(Lucene.Net.Search.StringIndex fcsi, int inclusiveLowerPoint, int inclusiveUpperPoint, FieldCacheRangeFilter<string> enclosingInstance)
                {
                    this.fcsi = fcsi;
                    this.inclusiveLowerPoint = inclusiveLowerPoint;
                    this.inclusiveUpperPoint = inclusiveUpperPoint;
                    this.enclosingInstance = enclosingInstance;
                }
                private Lucene.Net.Search.StringIndex fcsi;
                private int inclusiveLowerPoint;
                private int inclusiveUpperPoint;
                private FieldCacheRangeFilter<string> enclosingInstance;
                public FieldCacheRangeFilter<string> Enclosing_Instance
                {
                    get
                    {
                        return enclosingInstance;
                    }

                }
                internal AnonymousClassFieldCacheDocIdSet(Lucene.Net.Search.StringIndex fcsi, int inclusiveLowerPoint, int inclusiveUpperPoint, FieldCacheRangeFilter<string> enclosingInstance, Lucene.Net.Index.IndexReader Param1, bool Param2)
                    : base(Param1, Param2)
                {
                    InitBlock(fcsi, inclusiveLowerPoint, inclusiveUpperPoint, enclosingInstance);
                }
                internal override bool MatchDoc(int doc)
                {
                    return fcsi.order[doc] >= inclusiveLowerPoint && fcsi.order[doc] <= inclusiveUpperPoint;
                }
            }
            internal AnonymousClassFieldCacheRangeFilter(string field, Lucene.Net.Search.Parser parser, string lowerVal, string upperVal, bool includeLower, bool includeUpper)
                : base(field, parser, lowerVal, upperVal, includeLower, includeUpper)
            {
            }
            public override DocIdSet GetDocIdSet(IndexReader reader, IState state)
            {
                Lucene.Net.Search.StringIndex fcsi = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetStringIndex(reader, field, state);
                int lowerPoint = fcsi.BinarySearchLookup(lowerVal);
                int upperPoint = fcsi.BinarySearchLookup(upperVal);

                int inclusiveLowerPoint;
                int inclusiveUpperPoint;

                // Hints:
                // * binarySearchLookup returns 0, if value was null.
                // * the value is <0 if no exact hit was found, the returned value
                //   is (-(insertion point) - 1)
                if (lowerPoint == 0)
                {
                    System.Diagnostics.Debug.Assert(lowerVal == null);
                    inclusiveLowerPoint = 1;
                }
                else if (includeLower && lowerPoint > 0)
                {
                    inclusiveLowerPoint = lowerPoint;
                }
                else if (lowerPoint > 0)
                {
                    inclusiveLowerPoint = lowerPoint + 1;
                }
                else
                {
                    inclusiveLowerPoint = System.Math.Max(1, -lowerPoint - 1);
                }

                if (upperPoint == 0)
                {
                    System.Diagnostics.Debug.Assert(upperVal == null);
                    inclusiveUpperPoint = System.Int32.MaxValue;
                }
                else if (includeUpper && upperPoint > 0)
                {
                    inclusiveUpperPoint = upperPoint;
                }
                else if (upperPoint > 0)
                {
                    inclusiveUpperPoint = upperPoint - 1;
                }
                else
                {
                    inclusiveUpperPoint = -upperPoint - 2;
                }

                if (inclusiveUpperPoint <= 0 || inclusiveLowerPoint > inclusiveUpperPoint)
                    return DocIdSet.EMPTY_DOCIDSET;

                System.Diagnostics.Debug.Assert(inclusiveLowerPoint > 0 && inclusiveUpperPoint > 0);

                // for this DocIdSet, we never need to use TermDocs,
                // because deleted docs have an order of 0 (null entry in StringIndex)
                return new AnonymousClassFieldCacheDocIdSet(fcsi, inclusiveLowerPoint, inclusiveUpperPoint, this, reader, false);
            }
        }

        [Serializable]

        private class AnonymousClassFieldCacheRangeFilter1 : FieldCacheRangeFilter<sbyte?>
        {
            private class AnonymousClassFieldCacheDocIdSet : FieldCacheDocIdSet
            {
                private void InitBlock(sbyte[] values, sbyte inclusiveLowerPoint, sbyte inclusiveUpperPoint, FieldCacheRangeFilter<sbyte?> enclosingInstance)
                {
                    this.values = values;
                    this.inclusiveLowerPoint = inclusiveLowerPoint;
                    this.inclusiveUpperPoint = inclusiveUpperPoint;
                    this.enclosingInstance = enclosingInstance;
                }
                private sbyte[] values;
                private sbyte inclusiveLowerPoint;
                private sbyte inclusiveUpperPoint;
                private FieldCacheRangeFilter<sbyte?> enclosingInstance;
                public FieldCacheRangeFilter<sbyte?> Enclosing_Instance
                {
                    get
                    {
                        return enclosingInstance;
                    }

                }
                internal AnonymousClassFieldCacheDocIdSet(sbyte[] values, sbyte inclusiveLowerPoint, sbyte inclusiveUpperPoint, FieldCacheRangeFilter<sbyte?> enclosingInstance, Lucene.Net.Index.IndexReader Param1, bool Param2)
                    : base(Param1, Param2)
                {
                    InitBlock(values, inclusiveLowerPoint, inclusiveUpperPoint, enclosingInstance);
                }
                internal override bool MatchDoc(int doc)
                {
                    return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
                }
            }
            internal AnonymousClassFieldCacheRangeFilter1(string field, Parser parser, sbyte? lowerVal, sbyte? upperVal, bool includeLower, bool includeUpper)
                : base(field, parser, lowerVal, upperVal, includeLower, includeUpper)
            {
            }
            public override DocIdSet GetDocIdSet(IndexReader reader, IState state)
            {
                sbyte inclusiveLowerPoint;
                sbyte inclusiveUpperPoint;
                if (lowerVal != null)
                {
                    sbyte i = (sbyte)lowerVal;
                    if (!includeLower && i == sbyte.MaxValue)
                        return DocIdSet.EMPTY_DOCIDSET;
                    inclusiveLowerPoint = (sbyte)(includeLower ? i : (i + 1));
                }
                else
                {
                    inclusiveLowerPoint = sbyte.MinValue;
                }
                if (upperVal != null)
                {
                    sbyte i = (sbyte)upperVal;
                    if (!includeUpper && i == sbyte.MinValue)
                        return DocIdSet.EMPTY_DOCIDSET;
                    inclusiveUpperPoint = (sbyte)(includeUpper ? i : (i - 1));
                }
                else
                {
                    inclusiveUpperPoint = sbyte.MaxValue;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return DocIdSet.EMPTY_DOCIDSET;

                sbyte[] values = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetBytes(reader, field, (Lucene.Net.Search.ByteParser)parser, state);
                // we only request the usage of termDocs, if the range contains 0
                return new AnonymousClassFieldCacheDocIdSet(values, inclusiveLowerPoint, inclusiveUpperPoint, this, reader, (inclusiveLowerPoint <= 0 && inclusiveUpperPoint >= 0));
            }
        }

        [Serializable]

        private class AnonymousClassFieldCacheRangeFilter2 : FieldCacheRangeFilter<short?>
        {
            private class AnonymousClassFieldCacheDocIdSet : FieldCacheDocIdSet
            {
                private void InitBlock(short[] values, short inclusiveLowerPoint, short inclusiveUpperPoint, FieldCacheRangeFilter<short?> enclosingInstance)
                {
                    this.values = values;
                    this.inclusiveLowerPoint = inclusiveLowerPoint;
                    this.inclusiveUpperPoint = inclusiveUpperPoint;
                    this.enclosingInstance = enclosingInstance;
                }
                private short[] values;
                private short inclusiveLowerPoint;
                private short inclusiveUpperPoint;
                private FieldCacheRangeFilter<short?> enclosingInstance;
                public FieldCacheRangeFilter<short?> Enclosing_Instance
                {
                    get
                    {
                        return enclosingInstance;
                    }

                }
                internal AnonymousClassFieldCacheDocIdSet(short[] values, short inclusiveLowerPoint, short inclusiveUpperPoint, FieldCacheRangeFilter<short?> enclosingInstance, Lucene.Net.Index.IndexReader Param1, bool Param2)
                    : base(Param1, Param2)
                {
                    InitBlock(values, inclusiveLowerPoint, inclusiveUpperPoint, enclosingInstance);
                }
                internal override bool MatchDoc(int doc)
                {
                    return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
                }
            }
            internal AnonymousClassFieldCacheRangeFilter2(string field, Parser parser, short? lowerVal, short? upperVal, bool includeLower, bool includeUpper)
                : base(field, parser, lowerVal, upperVal, includeLower, includeUpper)
            {
            }
            public override DocIdSet GetDocIdSet(IndexReader reader, IState state)
            {
                short inclusiveLowerPoint;
                short inclusiveUpperPoint;
                if (lowerVal != null)
                {
                    short i = (short)lowerVal;
                    if (!includeLower && i == short.MaxValue)
                        return DocIdSet.EMPTY_DOCIDSET;
                    inclusiveLowerPoint = (short)(includeLower ? i : (i + 1));
                }
                else
                {
                    inclusiveLowerPoint = short.MinValue;
                }
                if (upperVal != null)
                {
                    short i = (short)upperVal;
                    if (!includeUpper && i == short.MinValue)
                        return DocIdSet.EMPTY_DOCIDSET;
                    inclusiveUpperPoint = (short)(includeUpper ? i : (i - 1));
                }
                else
                {
                    inclusiveUpperPoint = short.MaxValue;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return DocIdSet.EMPTY_DOCIDSET;

                short[] values = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetShorts(reader, field, (Lucene.Net.Search.ShortParser)parser, state);
                // we only request the usage of termDocs, if the range contains 0
                return new AnonymousClassFieldCacheDocIdSet(values, inclusiveLowerPoint, inclusiveUpperPoint, this, reader, (inclusiveLowerPoint <= 0 && inclusiveUpperPoint >= 0));
            }
        }

        [Serializable]

        private class AnonymousClassFieldCacheRangeFilter3 : FieldCacheRangeFilter<int?>
        {
            private class AnonymousClassFieldCacheDocIdSet : FieldCacheDocIdSet
            {
                private void InitBlock(int[] values, int inclusiveLowerPoint, int inclusiveUpperPoint, FieldCacheRangeFilter<int?> enclosingInstance)
                {
                    this.values = values;
                    this.inclusiveLowerPoint = inclusiveLowerPoint;
                    this.inclusiveUpperPoint = inclusiveUpperPoint;
                    this.enclosingInstance = enclosingInstance;
                }
                private int[] values;
                private int inclusiveLowerPoint;
                private int inclusiveUpperPoint;
                private FieldCacheRangeFilter<int?> enclosingInstance;
                public FieldCacheRangeFilter<int?> Enclosing_Instance
                {
                    get
                    {
                        return enclosingInstance;
                    }

                }
                internal AnonymousClassFieldCacheDocIdSet(int[] values, int inclusiveLowerPoint, int inclusiveUpperPoint, FieldCacheRangeFilter<int?> enclosingInstance, Lucene.Net.Index.IndexReader Param1, bool Param2)
                    : base(Param1, Param2)
                {
                    InitBlock(values, inclusiveLowerPoint, inclusiveUpperPoint, enclosingInstance);
                }
                internal override bool MatchDoc(int doc)
                {
                    return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
                }
            }
            internal AnonymousClassFieldCacheRangeFilter3(string field, Lucene.Net.Search.Parser parser, int? lowerVal, int? upperVal, bool includeLower, bool includeUpper)
                : base(field, parser, lowerVal, upperVal, includeLower, includeUpper)
            {
            }
            public override DocIdSet GetDocIdSet(IndexReader reader, IState state)
            {
                int inclusiveLowerPoint;
                int inclusiveUpperPoint;
                if (lowerVal != null)
                {
                    int i = (int)lowerVal;
                    if (!includeLower && i == int.MaxValue)
                        return DocIdSet.EMPTY_DOCIDSET;
                    inclusiveLowerPoint = includeLower ? i : (i + 1);
                }
                else
                {
                    inclusiveLowerPoint = int.MinValue;
                }
                if (upperVal != null)
                {
                    int i = (int)upperVal;
                    if (!includeUpper && i == int.MinValue)
                        return DocIdSet.EMPTY_DOCIDSET;
                    inclusiveUpperPoint = includeUpper ? i : (i - 1);
                }
                else
                {
                    inclusiveUpperPoint = int.MaxValue;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return DocIdSet.EMPTY_DOCIDSET;

                int[] values = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetInts(reader, field, (Lucene.Net.Search.IntParser)parser, state);
                // we only request the usage of termDocs, if the range contains 0
                return new AnonymousClassFieldCacheDocIdSet(values, inclusiveLowerPoint, inclusiveUpperPoint, this, reader, (inclusiveLowerPoint <= 0 && inclusiveUpperPoint >= 0));
            }
        }

        [Serializable]

        private class AnonymousClassFieldCacheRangeFilter4 : FieldCacheRangeFilter<long?>
        {
            private class AnonymousClassFieldCacheDocIdSet : FieldCacheDocIdSet
            {
                private void InitBlock(long[] values, long inclusiveLowerPoint, long inclusiveUpperPoint, FieldCacheRangeFilter<long?> enclosingInstance)
                {
                    this.values = values;
                    this.inclusiveLowerPoint = inclusiveLowerPoint;
                    this.inclusiveUpperPoint = inclusiveUpperPoint;
                    this.enclosingInstance = enclosingInstance;
                }
                private long[] values;
                private long inclusiveLowerPoint;
                private long inclusiveUpperPoint;
                private FieldCacheRangeFilter<long?> enclosingInstance;
                public FieldCacheRangeFilter<long?> Enclosing_Instance
                {
                    get
                    {
                        return enclosingInstance;
                    }

                }
                internal AnonymousClassFieldCacheDocIdSet(long[] values, long inclusiveLowerPoint, long inclusiveUpperPoint, FieldCacheRangeFilter<long?> enclosingInstance, Lucene.Net.Index.IndexReader Param1, bool Param2)
                    : base(Param1, Param2)
                {
                    InitBlock(values, inclusiveLowerPoint, inclusiveUpperPoint, enclosingInstance);
                }
                internal override bool MatchDoc(int doc)
                {
                    return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
                }
            }
            internal AnonymousClassFieldCacheRangeFilter4(string field, Lucene.Net.Search.Parser parser, long? lowerVal, long? upperVal, bool includeLower, bool includeUpper)
                : base(field, parser, lowerVal, upperVal, includeLower, includeUpper)
            {
            }
            public override DocIdSet GetDocIdSet(IndexReader reader, IState state)
            {
                long inclusiveLowerPoint;
                long inclusiveUpperPoint;
                if (lowerVal != null)
                {
                    long i = (long)lowerVal;
                    if (!includeLower && i == long.MaxValue)
                        return DocIdSet.EMPTY_DOCIDSET;
                    inclusiveLowerPoint = includeLower ? i : (i + 1L);
                }
                else
                {
                    inclusiveLowerPoint = long.MinValue;
                }
                if (upperVal != null)
                {
                    long i = (long)upperVal;
                    if (!includeUpper && i == long.MinValue)
                        return DocIdSet.EMPTY_DOCIDSET;
                    inclusiveUpperPoint = includeUpper ? i : (i - 1L);
                }
                else
                {
                    inclusiveUpperPoint = long.MaxValue;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return DocIdSet.EMPTY_DOCIDSET;

                long[] values = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetLongs(reader, field, (Lucene.Net.Search.LongParser)parser, state);
                // we only request the usage of termDocs, if the range contains 0
                return new AnonymousClassFieldCacheDocIdSet(values, inclusiveLowerPoint, inclusiveUpperPoint, this, reader, (inclusiveLowerPoint <= 0L && inclusiveUpperPoint >= 0L));
            }
        }

        [Serializable]

        private class AnonymousClassFieldCacheRangeFilter5 : FieldCacheRangeFilter<float?>
        {
            private class AnonymousClassFieldCacheDocIdSet : FieldCacheDocIdSet
            {
                private void InitBlock(float[] values, float inclusiveLowerPoint, float inclusiveUpperPoint, FieldCacheRangeFilter<float?> enclosingInstance)
                {
                    this.values = values;
                    this.inclusiveLowerPoint = inclusiveLowerPoint;
                    this.inclusiveUpperPoint = inclusiveUpperPoint;
                    this.enclosingInstance = enclosingInstance;
                }
                private float[] values;
                private float inclusiveLowerPoint;
                private float inclusiveUpperPoint;
                private FieldCacheRangeFilter<float?> enclosingInstance;
                public FieldCacheRangeFilter<float?> Enclosing_Instance
                {
                    get
                    {
                        return enclosingInstance;
                    }

                }
                internal AnonymousClassFieldCacheDocIdSet(float[] values, float inclusiveLowerPoint, float inclusiveUpperPoint, FieldCacheRangeFilter<float?> enclosingInstance, Lucene.Net.Index.IndexReader Param1, bool Param2)
                    : base(Param1, Param2)
                {
                    InitBlock(values, inclusiveLowerPoint, inclusiveUpperPoint, enclosingInstance);
                }
                internal override bool MatchDoc(int doc)
                {
                    return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
                }
            }
            internal AnonymousClassFieldCacheRangeFilter5(string field, Lucene.Net.Search.Parser parser, float? lowerVal, float? upperVal, bool includeLower, bool includeUpper)
                : base(field, parser, lowerVal, upperVal, includeLower, includeUpper)
            {
            }
            public override DocIdSet GetDocIdSet(IndexReader reader, IState state)
            {
                // we transform the floating point numbers to sortable integers
                // using NumericUtils to easier find the next bigger/lower value
                float inclusiveLowerPoint;
                float inclusiveUpperPoint;
                if (lowerVal != null)
                {
                    float f = (float)lowerVal;
                    if (!includeUpper && f > 0.0f && float.IsInfinity(f))
                        return DocIdSet.EMPTY_DOCIDSET;
                    int i = NumericUtils.FloatToSortableInt(f);
                    inclusiveLowerPoint = NumericUtils.SortableIntToFloat(includeLower ? i : (i + 1));
                }
                else
                {
                    inclusiveLowerPoint = float.NegativeInfinity;
                }
                if (upperVal != null)
                {
                    float f = (float)upperVal;
                    if (!includeUpper && f < 0.0f && float.IsInfinity(f))
                        return DocIdSet.EMPTY_DOCIDSET;
                    int i = NumericUtils.FloatToSortableInt(f);
                    inclusiveUpperPoint = NumericUtils.SortableIntToFloat(includeUpper ? i : (i - 1));
                }
                else
                {
                    inclusiveUpperPoint = float.PositiveInfinity;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return DocIdSet.EMPTY_DOCIDSET;

                float[] values = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetFloats(reader, field, (Lucene.Net.Search.FloatParser)parser, state);
                // we only request the usage of termDocs, if the range contains 0
                return new AnonymousClassFieldCacheDocIdSet(values, inclusiveLowerPoint, inclusiveUpperPoint, this, reader, (inclusiveLowerPoint <= 0.0f && inclusiveUpperPoint >= 0.0f));
            }
        }

        [Serializable]

        private class AnonymousClassFieldCacheRangeFilter6 : FieldCacheRangeFilter<double?>
        {
            private class AnonymousClassFieldCacheDocIdSet : FieldCacheDocIdSet
            {
                private void InitBlock(double[] values, double inclusiveLowerPoint, double inclusiveUpperPoint, FieldCacheRangeFilter<double?> enclosingInstance)
                {
                    this.values = values;
                    this.inclusiveLowerPoint = inclusiveLowerPoint;
                    this.inclusiveUpperPoint = inclusiveUpperPoint;
                    this.enclosingInstance = enclosingInstance;
                }
                private double[] values;
                private double inclusiveLowerPoint;
                private double inclusiveUpperPoint;
                private FieldCacheRangeFilter<double?> enclosingInstance;
                public FieldCacheRangeFilter<double?> Enclosing_Instance
                {
                    get
                    {
                        return enclosingInstance;
                    }

                }
                internal AnonymousClassFieldCacheDocIdSet(double[] values, double inclusiveLowerPoint, double inclusiveUpperPoint, FieldCacheRangeFilter<double?> enclosingInstance, Lucene.Net.Index.IndexReader Param1, bool Param2)
                    : base(Param1, Param2)
                {
                    InitBlock(values, inclusiveLowerPoint, inclusiveUpperPoint, enclosingInstance);
                }
                internal override bool MatchDoc(int doc)
                {
                    return values[doc] >= inclusiveLowerPoint && values[doc] <= inclusiveUpperPoint;
                }
            }
            internal AnonymousClassFieldCacheRangeFilter6(string field, Lucene.Net.Search.Parser parser, double? lowerVal, double? upperVal, bool includeLower, bool includeUpper)
                : base(field, parser, lowerVal, upperVal, includeLower, includeUpper)
            {
            }
            public override DocIdSet GetDocIdSet(IndexReader reader, IState state)
            {
                // we transform the floating point numbers to sortable integers
                // using NumericUtils to easier find the next bigger/lower value
                double inclusiveLowerPoint;
                double inclusiveUpperPoint;
                if (lowerVal != null)
                {
                    double f = (double)lowerVal;
                    if (!includeUpper && f > 0.0 && double.IsInfinity(f))
                        return DocIdSet.EMPTY_DOCIDSET;
                    long i = NumericUtils.DoubleToSortableLong(f);
                    inclusiveLowerPoint = NumericUtils.SortableLongToDouble(includeLower ? i : (i + 1L));
                }
                else
                {
                    inclusiveLowerPoint = double.NegativeInfinity;
                }
                if (upperVal != null)
                {
                    double f = (double)upperVal;
                    if (!includeUpper && f < 0.0 && double.IsInfinity(f))
                        return DocIdSet.EMPTY_DOCIDSET;
                    long i = NumericUtils.DoubleToSortableLong(f);
                    inclusiveUpperPoint = NumericUtils.SortableLongToDouble(includeUpper ? i : (i - 1L));
                }
                else
                {
                    inclusiveUpperPoint = double.PositiveInfinity;
                }

                if (inclusiveLowerPoint > inclusiveUpperPoint)
                    return DocIdSet.EMPTY_DOCIDSET;

                double[] values = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetDoubles(reader, field, (Lucene.Net.Search.DoubleParser)parser, state);
                // we only request the usage of termDocs, if the range contains 0
                return new AnonymousClassFieldCacheDocIdSet(values, inclusiveLowerPoint, inclusiveUpperPoint, this, reader, (inclusiveLowerPoint <= 0.0 && inclusiveUpperPoint >= 0.0));
            }
        }

        /// <summary> Creates a string range filter using <see cref="FieldCache.GetStringIndex(IndexReader,string)" />. This works with all
        /// fields containing zero or one term in the field. The range can be half-open by setting one
        /// of the values to <c>null</c>.
        /// </summary>
        public static FieldCacheRangeFilter<string> NewStringRange(string field, string lowerVal, string upperVal, bool includeLower, bool includeUpper)
        {
            return new AnonymousClassFieldCacheRangeFilter(field, null, lowerVal, upperVal, includeLower, includeUpper);
        }

        /// <summary> Creates a numeric range filter using <see cref="FieldCache.GetBytes(IndexReader,String)" />. This works with all
        /// byte fields containing exactly one numeric term in the field. The range can be half-open by setting one
        /// of the values to <c>null</c>.
        /// </summary>
        public static FieldCacheRangeFilter<sbyte?> NewByteRange(string field, sbyte? lowerVal, sbyte? upperVal, bool includeLower, bool includeUpper)
        {
            return NewByteRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
        }

        /// <summary> Creates a numeric range filter using <see cref="FieldCache.GetBytes(IndexReader,String,ByteParser)" />. This works with all
        /// byte fields containing exactly one numeric term in the field. The range can be half-open by setting one
        /// of the values to <c>null</c>.
        /// </summary>
        public static FieldCacheRangeFilter<sbyte?> NewByteRange(string field, Lucene.Net.Search.ByteParser parser, sbyte? lowerVal, sbyte? upperVal, bool includeLower, bool includeUpper)
        {
            return new AnonymousClassFieldCacheRangeFilter1(field, parser, lowerVal, upperVal, includeLower, includeUpper);
        }

        /// <summary> Creates a numeric range query using <see cref="FieldCache.GetShorts(IndexReader,String)" />. This works with all
        /// short fields containing exactly one numeric term in the field. The range can be half-open by setting one
        /// of the values to <c>null</c>.
        /// </summary>
        public static FieldCacheRangeFilter<short?> NewShortRange(string field, short? lowerVal, short? upperVal, bool includeLower, bool includeUpper)
        {
            return NewShortRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
        }

        /// <summary> Creates a numeric range query using <see cref="FieldCache.GetShorts(IndexReader,String,ShortParser)" />. This works with all
        /// short fields containing exactly one numeric term in the field. The range can be half-open by setting one
        /// of the values to <c>null</c>.
        /// </summary>
        public static FieldCacheRangeFilter<short?> NewShortRange(string field, Lucene.Net.Search.ShortParser parser, short? lowerVal, short? upperVal, bool includeLower, bool includeUpper)
        {
            return new AnonymousClassFieldCacheRangeFilter2(field, parser, lowerVal, upperVal, includeLower, includeUpper);
        }

        /// <summary> Creates a numeric range query using <see cref="FieldCache.GetInts(IndexReader,String)" />. This works with all
        /// int fields containing exactly one numeric term in the field. The range can be half-open by setting one
        /// of the values to <c>null</c>.
        /// </summary>
        public static FieldCacheRangeFilter<int?> NewIntRange(string field, int? lowerVal, int? upperVal, bool includeLower, bool includeUpper)
        {
            return NewIntRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
        }

        /// <summary> Creates a numeric range query using <see cref="FieldCache.GetInts(IndexReader,String,IntParser)" />. This works with all
        /// int fields containing exactly one numeric term in the field. The range can be half-open by setting one
        /// of the values to <c>null</c>.
        /// </summary>
        public static FieldCacheRangeFilter<int?> NewIntRange(string field, Lucene.Net.Search.IntParser parser, int? lowerVal, int? upperVal, bool includeLower, bool includeUpper)
        {
            return new AnonymousClassFieldCacheRangeFilter3(field, parser, lowerVal, upperVal, includeLower, includeUpper);
        }

        /// <summary> Creates a numeric range query using <see cref="FieldCache.GetLongs(IndexReader,String)" />. This works with all
        /// long fields containing exactly one numeric term in the field. The range can be half-open by setting one
        /// of the values to <c>null</c>.
        /// </summary>
        public static FieldCacheRangeFilter<long?> NewLongRange(string field, long? lowerVal, long? upperVal, bool includeLower, bool includeUpper)
        {
            return NewLongRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
        }

        /// <summary> Creates a numeric range query using <see cref="FieldCache.GetLongs(IndexReader,String,LongParser)" />. This works with all
        /// long fields containing exactly one numeric term in the field. The range can be half-open by setting one
        /// of the values to <c>null</c>.
        /// </summary>
        public static FieldCacheRangeFilter<long?> NewLongRange(string field, Lucene.Net.Search.LongParser parser, long? lowerVal, long? upperVal, bool includeLower, bool includeUpper)
        {
            return new AnonymousClassFieldCacheRangeFilter4(field, parser, lowerVal, upperVal, includeLower, includeUpper);
        }

        /// <summary> Creates a numeric range query using <see cref="FieldCache.GetFloats(IndexReader,String)" />. This works with all
        /// float fields containing exactly one numeric term in the field. The range can be half-open by setting one
        /// of the values to <c>null</c>.
        /// </summary>
        public static FieldCacheRangeFilter<float?> NewFloatRange(string field, float? lowerVal, float? upperVal, bool includeLower, bool includeUpper)
        {
            return NewFloatRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
        }

        /// <summary> Creates a numeric range query using <see cref="FieldCache.GetFloats(IndexReader,String,FloatParser)" />. This works with all
        /// float fields containing exactly one numeric term in the field. The range can be half-open by setting one
        /// of the values to <c>null</c>.
        /// </summary>
        public static FieldCacheRangeFilter<float?> NewFloatRange(string field, Lucene.Net.Search.FloatParser parser, float? lowerVal, float? upperVal, bool includeLower, bool includeUpper)
        {
            return new AnonymousClassFieldCacheRangeFilter5(field, parser, lowerVal, upperVal, includeLower, includeUpper);
        }

        /// <summary> Creates a numeric range query using <see cref="FieldCache.GetDoubles(IndexReader,String)" />. This works with all
        /// double fields containing exactly one numeric term in the field. The range can be half-open by setting one
        /// of the values to <c>null</c>.
        /// </summary>
        public static FieldCacheRangeFilter<double?> NewDoubleRange(string field, double? lowerVal, double? upperVal, bool includeLower, bool includeUpper)
        {
            return NewDoubleRange(field, null, lowerVal, upperVal, includeLower, includeUpper);
        }

        /// <summary> Creates a numeric range query using <see cref="FieldCache.GetDoubles(IndexReader,String,DoubleParser)" />. This works with all
        /// double fields containing exactly one numeric term in the field. The range can be half-open by setting one
        /// of the values to <c>null</c>.
        /// </summary>
        public static FieldCacheRangeFilter<double?> NewDoubleRange(string field, Lucene.Net.Search.DoubleParser parser, double? lowerVal, double? upperVal, bool includeLower, bool includeUpper)
        {
            return new AnonymousClassFieldCacheRangeFilter6(field, parser, lowerVal, upperVal, includeLower, includeUpper);
        }
	}


        [Serializable]

    public abstract class FieldCacheRangeFilter<T> : Filter
	{
		internal System.String field;
		internal Lucene.Net.Search.Parser parser;
		internal T lowerVal;
		internal T upperVal;
		internal bool includeLower;
		internal bool includeUpper;
		
		protected internal FieldCacheRangeFilter(System.String field, Lucene.Net.Search.Parser parser, T lowerVal, T upperVal, bool includeLower, bool includeUpper)
		{
			this.field = field;
			this.parser = parser;
			this.lowerVal = lowerVal;
			this.upperVal = upperVal;
			this.includeLower = includeLower;
			this.includeUpper = includeUpper;
		}
		
		/// <summary>This method is implemented for each data type </summary>
		public abstract override DocIdSet GetDocIdSet(IndexReader reader, IState state);
		
		public override System.String ToString()
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(field).Append(":");
			return sb.Append(includeLower?'[':'{').Append((lowerVal == null)?"*":lowerVal.ToString()).Append(" TO ").Append((upperVal == null)?"*":upperVal.ToString()).Append(includeUpper?']':'}').ToString();
		}
		
		public  override bool Equals(System.Object o)
		{
			if (this == o)
				return true;
			if (!(o is FieldCacheRangeFilter<T>))
				return false;
			FieldCacheRangeFilter<T> other = (FieldCacheRangeFilter<T>) o;
			
			if (!this.field.Equals(other.field) || this.includeLower != other.includeLower || this.includeUpper != other.includeUpper)
			{
				return false;
			}
			if (this.lowerVal != null ?! this.lowerVal.Equals(other.lowerVal):other.lowerVal != null)
				return false;
			if (this.upperVal != null ?! this.upperVal.Equals(other.upperVal):other.upperVal != null)
				return false;
			if (this.parser != null ?! this.parser.Equals(other.parser):other.parser != null)
				return false;
			return true;
		}
		
		public override int GetHashCode()
		{
			int h = field.GetHashCode();
			h ^= ((lowerVal != null)?lowerVal.GetHashCode():550356204);
			h = (h << 1) | (Number.URShift(h, 31)); // rotate to distinguish lower from upper
			h ^= ((upperVal != null)?upperVal.GetHashCode():- 1674416163);
			h ^= ((parser != null)?parser.GetHashCode():- 1572457324);
			h ^= (includeLower?1549299360:- 365038026) ^ (includeUpper?1721088258:1948649653);
			return h;
		}
		
        /// <summary>
        /// Returns the field name for this filter
        /// </summary>
        public string GetField { get { return field; } }

        /// <summary>
        /// Returns <c>true</c> if the lower endpoint is inclusive
        /// </summary>
        public bool IncludesLower { get { return includeLower; } }

        /// <summary>
        /// Returns <c>true</c> if the upper endpoint is inclusive
        /// </summary>
        public bool IncludesUpper { get { return includeUpper; } }

        /// <summary>
        /// Returns the lower value of the range filter
        /// </summary>
	    public T LowerValue { get { return lowerVal; } }

        /// <summary>
        /// Returns the upper value of this range filter
        /// </summary>
        public T UpperValue { get { return upperVal; } }

        public Parser Parser { get { return parser; } }

		internal abstract class FieldCacheDocIdSet:DocIdSet
		{
			private class AnonymousClassDocIdSetIterator : DocIdSetIterator
			{
				public AnonymousClassDocIdSetIterator(Lucene.Net.Index.TermDocs termDocs, FieldCacheDocIdSet enclosingInstance)
				{
					InitBlock(termDocs, enclosingInstance);
				}
				private void  InitBlock(Lucene.Net.Index.TermDocs termDocs, FieldCacheDocIdSet enclosingInstance)
				{
					this.termDocs = termDocs;
					this.enclosingInstance = enclosingInstance;
				}
				private Lucene.Net.Index.TermDocs termDocs;
				private FieldCacheDocIdSet enclosingInstance;
				public FieldCacheDocIdSet Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				private int doc = - 1;
				
				public override int DocID()
				{
					return doc;
				}
				
				public override int NextDoc(IState state)
				{
					do 
					{
						if (!termDocs.Next(state))
							return doc = NO_MORE_DOCS;
					}
					while (!Enclosing_Instance.MatchDoc(doc = termDocs.Doc));
					return doc;
				}
				
				public override int Advance(int target, IState state)
				{
					if (!termDocs.SkipTo(target, state))
						return doc = NO_MORE_DOCS;
					while (!Enclosing_Instance.MatchDoc(doc = termDocs.Doc))
					{
						if (!termDocs.Next(state))
							return doc = NO_MORE_DOCS;
					}
					return doc;
				}
			}
			private class AnonymousClassDocIdSetIterator1:DocIdSetIterator
			{
				public AnonymousClassDocIdSetIterator1(FieldCacheDocIdSet enclosingInstance)
				{
					InitBlock(enclosingInstance);
				}
				private void  InitBlock(FieldCacheDocIdSet enclosingInstance)
				{
					this.enclosingInstance = enclosingInstance;
				}
				private FieldCacheDocIdSet enclosingInstance;
				public FieldCacheDocIdSet Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				private int doc = - 1;
				
				public override int DocID()
				{
					return doc;
				}
				
				public override int NextDoc(IState state)
				{
					try
					{
						do 
						{
							doc++;
						}
						while (!Enclosing_Instance.MatchDoc(doc));
						return doc;
					}
					catch (System.IndexOutOfRangeException)
					{
						return doc = NO_MORE_DOCS;
					}
				}
				
				public override int Advance(int target, IState state)
				{
					try
					{
						doc = target;
						while (!Enclosing_Instance.MatchDoc(doc))
						{
							doc++;
						}
						return doc;
					}
					catch (System.IndexOutOfRangeException)
					{
						return doc = NO_MORE_DOCS;
					}
				}
			}
			private IndexReader reader;
			private bool mayUseTermDocs;
			
			internal FieldCacheDocIdSet(IndexReader reader, bool mayUseTermDocs)
			{
				this.reader = reader;
				this.mayUseTermDocs = mayUseTermDocs;
			}
			
			/// <summary>this method checks, if a doc is a hit, should throw AIOBE, when position invalid </summary>
			internal abstract bool MatchDoc(int doc);

		    /// <summary>this DocIdSet is cacheable, if it works solely with FieldCache and no TermDocs </summary>
		    public override bool IsCacheable
		    {
		        get { return !(mayUseTermDocs && reader.HasDeletions); }
		    }

		    public override DocIdSetIterator Iterator(IState state)
			{
				// Synchronization needed because deleted docs BitVector
				// can change after call to hasDeletions until TermDocs creation.
				// We only use an iterator with termDocs, when this was requested (e.g. range contains 0)
				// and the index has deletions
				TermDocs termDocs;
				lock (reader)
				{
					termDocs = IsCacheable ? null : reader.TermDocs(null, state);
				}
				if (termDocs != null)
				{
					// a DocIdSetIterator using TermDocs to iterate valid docIds
					return new AnonymousClassDocIdSetIterator(termDocs, this);
				}
				else
				{
					// a DocIdSetIterator generating docIds by incrementing a variable -
					// this one can be used if there are no deletions are on the index
					return new AnonymousClassDocIdSetIterator1(this);
				}
			}
		}
	}
}