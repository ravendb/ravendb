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
using System.IO;
using Lucene.Net.Search;
using Lucene.Net.Store;
using NumericTokenStream = Lucene.Net.Analysis.NumericTokenStream;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using NumericUtils = Lucene.Net.Util.NumericUtils;
using FieldCache = Lucene.Net.Search.FieldCache;
using SortField = Lucene.Net.Search.SortField;

namespace Lucene.Net.Documents
{
    // javadocs

    /// <summary> <p/>This class provides a <see cref="Field" /> that enables indexing
    /// of numeric values for efficient range filtering and
    /// sorting.  Here's an example usage, adding an int value:
    /// <code>
    /// document.add(new NumericField(name).setIntValue(value));
    /// </code>
    /// 
    /// For optimal performance, re-use the
    /// <c>NumericField</c> and <see cref="Document" /> instance for more than
    /// one document:
    /// 
    /// <code>
    /// NumericField field = new NumericField(name);
    /// Document document = new Document();
    /// document.add(field);
    /// 
    /// for(all documents) {
    /// ...
    /// field.setIntValue(value)
    /// writer.addDocument(document);
    /// ...
    /// }
    /// </code>
    /// 
    /// <p/>The .Net native types <c>int</c>, <c>long</c>,
    /// <c>float</c> and <c>double</c> are
    /// directly supported.  However, any value that can be
    /// converted into these native types can also be indexed.
    /// For example, date/time values represented by a
    /// <see cref="System.DateTime" /> can be translated into a long
    /// value using the <c>java.util.Date.getTime</c> method.  If you
    /// don't need millisecond precision, you can quantize the
    /// value, either by dividing the result of
    /// <c>java.util.Date.getTime</c> or using the separate getters
    /// (for year, month, etc.) to construct an <c>int</c> or
    /// <c>long</c> value.<p/>
    /// 
    /// <p/>To perform range querying or filtering against a
    /// <c>NumericField</c>, use <see cref="NumericRangeQuery{T}" /> or <see cref="NumericRangeFilter{T}" />
    ///.  To sort according to a
    /// <c>NumericField</c>, use the normal numeric sort types, eg
    /// <see cref="SortField.INT" />  <c>NumericField</c> values
    /// can also be loaded directly from <see cref="FieldCache" />.<p/>
    /// 
    /// <p/>By default, a <c>NumericField</c>'s value is not stored but
    /// is indexed for range filtering and sorting.  You can use
    /// the <see cref="NumericField(String,Field.Store,bool)" />
    /// constructor if you need to change these defaults.<p/>
    /// 
    /// <p/>You may add the same field name as a <c>NumericField</c> to
    /// the same document more than once.  Range querying and
    /// filtering will be the logical OR of all values; so a range query
    /// will hit all documents that have at least one value in
    /// the range. However sort behavior is not defined.  If you need to sort,
    /// you should separately index a single-valued <c>NumericField</c>.<p/>
    /// 
    /// <p/>A <c>NumericField</c> will consume somewhat more disk space
    /// in the index than an ordinary single-valued field.
    /// However, for a typical index that includes substantial
    /// textual content per document, this increase will likely
    /// be in the noise. <p/>
    /// 
    /// <p/>Within Lucene, each numeric value is indexed as a
    /// <em>trie</em> structure, where each term is logically
    /// assigned to larger and larger pre-defined brackets (which
    /// are simply lower-precision representations of the value).
    /// The step size between each successive bracket is called the
    /// <c>precisionStep</c>, measured in bits.  Smaller
    /// <c>precisionStep</c> values result in larger number
    /// of brackets, which consumes more disk space in the index
    /// but may result in faster range search performance.  The
    /// default value, 4, was selected for a reasonable tradeoff
    /// of disk space consumption versus performance.  You can
    /// use the expert constructor <see cref="NumericField(String,int,Field.Store,bool)" />
    /// if you'd
    /// like to change the value.  Note that you must also
    /// specify a congruent value when creating <see cref="NumericRangeQuery{T}" />
    /// or <see cref="NumericRangeFilter{T}" />.
    /// For low cardinality fields larger precision steps are good.
    /// If the cardinality is &lt; 100, it is fair
    /// to use <see cref="int.MaxValue" />, which produces one
    /// term per value.
    /// 
    /// <p/>For more information on the internals of numeric trie
    /// indexing, including the <a
    /// href="../search/NumericRangeQuery.html#precisionStepDesc"><c>precisionStep</c></a>
    /// configuration, see <see cref="NumericRangeQuery{T}" />. The format of
    /// indexed values is described in <see cref="NumericUtils" />.
    /// 
    /// <p/>If you only need to sort by numeric value, and never
    /// run range querying/filtering, you can index using a
    /// <c>precisionStep</c> of <see cref="int.MaxValue" />.
    /// This will minimize disk space consumed. <p/>
    /// 
    /// <p/>More advanced users can instead use <see cref="NumericTokenStream" />
    /// directly, when indexing numbers. This
    /// class is a wrapper around this token stream type for
    /// easier, more intuitive usage.<p/>
    /// 
    /// <p/><b>NOTE:</b> This class is only used during
    /// indexing. When retrieving the stored field value from a
    /// <see cref="Document" /> instance after search, you will get a
    /// conventional <see cref="IFieldable" /> instance where the numeric
    /// values are returned as <see cref="String" />s (according to
    /// <c>toString(value)</c> of the used data type).
    /// 
    /// <p/><font color="red"><b>NOTE:</b> This API is
    /// experimental and might change in incompatible ways in the
    /// next release.</font>
    /// 
    /// </summary>
    /// <since> 2.9
    /// </since>
    [Serializable]
    public sealed class NumericField:AbstractField
	{
		
		new private readonly NumericTokenStream tokenStream;
		
		/// <summary> Creates a field for numeric values using the default <c>precisionStep</c>
		/// <see cref="NumericUtils.PRECISION_STEP_DEFAULT" /> (4). The instance is not yet initialized with
		/// a numeric value, before indexing a document containing this field,
		/// set a value using the various set<em>???</em>Value() methods.
		/// This constructor creates an indexed, but not stored field.
		/// </summary>
		/// <param name="name">the field name
		/// </param>
		public NumericField(System.String name):this(name, NumericUtils.PRECISION_STEP_DEFAULT, Field.Store.NO, true)
		{
		}
		
		/// <summary> Creates a field for numeric values using the default <c>precisionStep</c>
		/// <see cref="NumericUtils.PRECISION_STEP_DEFAULT" /> (4). The instance is not yet initialized with
		/// a numeric value, before indexing a document containing this field,
		/// set a value using the various set<em>???</em>Value() methods.
		/// </summary>
		/// <param name="name">the field name
		/// </param>
		/// <param name="store">if the field should be stored in plain text form
		/// (according to <c>toString(value)</c> of the used data type)
		/// </param>
		/// <param name="index">if the field should be indexed using <see cref="NumericTokenStream" />
		/// </param>
		public NumericField(System.String name, Field.Store store, bool index):this(name, NumericUtils.PRECISION_STEP_DEFAULT, store, index)
		{
		}
		
		/// <summary> Creates a field for numeric values with the specified
		/// <c>precisionStep</c>. The instance is not yet initialized with
		/// a numeric value, before indexing a document containing this field,
		/// set a value using the various set<em>???</em>Value() methods.
		/// This constructor creates an indexed, but not stored field.
		/// </summary>
		/// <param name="name">the field name
		/// </param>
		/// <param name="precisionStep">the used <a href="../search/NumericRangeQuery.html#precisionStepDesc">precision step</a>
		/// </param>
		public NumericField(System.String name, int precisionStep):this(name, precisionStep, Field.Store.NO, true)
		{
		}
		
		/// <summary> Creates a field for numeric values with the specified
		/// <c>precisionStep</c>. The instance is not yet initialized with
		/// a numeric value, before indexing a document containing this field,
		/// set a value using the various set<em>???</em>Value() methods.
		/// </summary>
		/// <param name="name">the field name
		/// </param>
		/// <param name="precisionStep">the used <a href="../search/NumericRangeQuery.html#precisionStepDesc">precision step</a>
		/// </param>
		/// <param name="store">if the field should be stored in plain text form
		/// (according to <c>toString(value)</c> of the used data type)
		/// </param>
		/// <param name="index">if the field should be indexed using <see cref="NumericTokenStream" />
		/// </param>
		public NumericField(System.String name, int precisionStep, Field.Store store, bool index):base(name, store, index?Field.Index.ANALYZED_NO_NORMS:Field.Index.NO, Field.TermVector.NO)
		{
			OmitTermFreqAndPositions = true;
			tokenStream = new NumericTokenStream(precisionStep);
		}

	    /// <summary>Returns a <see cref="NumericTokenStream" /> for indexing the numeric value. </summary>
	    public override TokenStream TokenStreamValue
	    {
	        get { return IsIndexed ? tokenStream : null; }
	    }

	    /// <summary>Returns always <c>null</c> for numeric fields </summary>
		public override byte[] GetBinaryValue(byte[] result, IState state)
		{
			return null;
		}

	    /// <summary>Returns always <c>null</c> for numeric fields </summary>
	    public override TextReader ReaderValue
	    {
	        get { return null; }
	    }

	    /// <summary>Returns the numeric value as a string (how it is stored, when <see cref="Field.Store.YES" /> is chosen). </summary>
	    public override string StringValue(IState state)
	    {
	        return (fieldsData == null) ? null : fieldsData.ToString();
	    }

	    /// <summary>Returns the current numeric value as a subclass of <see cref="Number" />, <c>null</c> if not yet initialized. </summary>
	    public ValueType NumericValue
	    {
	        get { return (System.ValueType) fieldsData; }
	    }

	    /// <summary> Initializes the field with the supplied <c>long</c> value.</summary>
		/// <param name="value_Renamed">the numeric value
		/// </param>
		/// <returns> this instance, because of this you can use it the following way:
		/// <c>document.add(new NumericField(name, precisionStep).SetLongValue(value))</c>
		/// </returns>
		public NumericField SetLongValue(long value_Renamed)
		{
			tokenStream.SetLongValue(value_Renamed);
			fieldsData = value_Renamed;
			return this;
		}
		
		/// <summary> Initializes the field with the supplied <c>int</c> value.</summary>
		/// <param name="value_Renamed">the numeric value
		/// </param>
		/// <returns> this instance, because of this you can use it the following way:
		/// <c>document.add(new NumericField(name, precisionStep).setIntValue(value))</c>
		/// </returns>
		public NumericField SetIntValue(int value_Renamed)
		{
			tokenStream.SetIntValue(value_Renamed);
			fieldsData = value_Renamed;
			return this;
		}
		
		/// <summary> Initializes the field with the supplied <c>double</c> value.</summary>
		/// <param name="value_Renamed">the numeric value
		/// </param>
		/// <returns> this instance, because of this you can use it the following way:
		/// <c>document.add(new NumericField(name, precisionStep).setDoubleValue(value))</c>
		/// </returns>
		public NumericField SetDoubleValue(double value_Renamed)
		{
			tokenStream.SetDoubleValue(value_Renamed);
			fieldsData = value_Renamed;
			return this;
		}
		
		/// <summary> Initializes the field with the supplied <c>float</c> value.</summary>
		/// <param name="value_Renamed">the numeric value
		/// </param>
		/// <returns> this instance, because of this you can use it the following way:
		/// <c>document.add(new NumericField(name, precisionStep).setFloatValue(value))</c>
		/// </returns>
		public NumericField SetFloatValue(float value_Renamed)
		{
			tokenStream.SetFloatValue(value_Renamed);
			fieldsData = value_Renamed;
			return this;
		}
	}
}