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
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Search;
using AttributeSource = Lucene.Net.Util.AttributeSource;
using NumericUtils = Lucene.Net.Util.NumericUtils;
using NumericField = Lucene.Net.Documents.NumericField;
// javadocs

namespace Lucene.Net.Analysis
{
	
	/// <summary> <b>Expert:</b> This class provides a <see cref="TokenStream" />
	/// for indexing numeric values that can be used by <see cref="NumericRangeQuery{T}" />
    /// or <see cref="NumericRangeFilter{T}" />.
	/// 
	/// <p/>Note that for simple usage, <see cref="NumericField" /> is
	/// recommended.  <see cref="NumericField" /> disables norms and
	/// term freqs, as they are not usually needed during
	/// searching.  If you need to change these settings, you
	/// should use this class.
	/// 
	/// <p/>See <see cref="NumericField" /> for capabilities of fields
	/// indexed numerically.<p/>
	/// 
	/// <p/>Here's an example usage, for an <c>int</c> field:
	/// 
	/// <code>
	///  Field field = new Field(name, new NumericTokenStream(precisionStep).setIntValue(value));
	///  field.setOmitNorms(true);
	///  field.setOmitTermFreqAndPositions(true);
	///  document.add(field);
	/// </code>
	/// 
	/// <p/>For optimal performance, re-use the TokenStream and Field instance
	/// for more than one document:
	/// 
	/// <code>
	///  NumericTokenStream stream = new NumericTokenStream(precisionStep);
	///  Field field = new Field(name, stream);
	///  field.setOmitNorms(true);
	///  field.setOmitTermFreqAndPositions(true);
	///  Document document = new Document();
	///  document.add(field);
	/// 
	///  for(all documents) {
	///    stream.setIntValue(value)
	///    writer.addDocument(document);
	///  }
	/// </code>
	/// 
	/// <p/>This stream is not intended to be used in analyzers;
	/// it's more for iterating the different precisions during
	/// indexing a specific numeric value.<p/>
	/// 
	/// <p/><b>NOTE</b>: as token streams are only consumed once
	/// the document is added to the index, if you index more
	/// than one numeric field, use a separate <c>NumericTokenStream</c>
	/// instance for each.<p/>
	/// 
    /// <p/>See <see cref="NumericRangeQuery{T}" /> for more details on the
	/// <a href="../search/NumericRangeQuery.html#precisionStepDesc"><c>precisionStep</c></a>
	/// parameter as well as how numeric fields work under the hood.<p/>
	/// 
	/// <p/><font color="red"><b>NOTE:</b> This API is experimental and
	/// might change in incompatible ways in the next release.</font>
	///   Since 2.9
	/// </summary>
	public sealed class NumericTokenStream : TokenStream
	{
		private void  InitBlock()
		{
            termAtt = AddAttribute<ITermAttribute>();
            typeAtt = AddAttribute<ITypeAttribute>();
            posIncrAtt = AddAttribute<IPositionIncrementAttribute>();
		}
		
		/// <summary>The full precision token gets this token type assigned. </summary>
		public const System.String TOKEN_TYPE_FULL_PREC = "fullPrecNumeric";
		
		/// <summary>The lower precision tokens gets this token type assigned. </summary>
		public const System.String TOKEN_TYPE_LOWER_PREC = "lowerPrecNumeric";
		
		/// <summary> Creates a token stream for numeric values using the default <c>precisionStep</c>
		/// <see cref="NumericUtils.PRECISION_STEP_DEFAULT" /> (4). The stream is not yet initialized,
		/// before using set a value using the various set<em>???</em>Value() methods.
		/// </summary>
		public NumericTokenStream():this(NumericUtils.PRECISION_STEP_DEFAULT)
		{
		}
		
		/// <summary> Creates a token stream for numeric values with the specified
		/// <c>precisionStep</c>. The stream is not yet initialized,
		/// before using set a value using the various set<em>???</em>Value() methods.
		/// </summary>
		public NumericTokenStream(int precisionStep):base()
		{
			InitBlock();
			this.precisionStep = precisionStep;
			if (precisionStep < 1)
				throw new System.ArgumentException("precisionStep must be >=1");
		}
		
		/// <summary> Expert: Creates a token stream for numeric values with the specified
		/// <c>precisionStep</c> using the given <see cref="AttributeSource" />.
		/// The stream is not yet initialized,
		/// before using set a value using the various set<em>???</em>Value() methods.
		/// </summary>
		public NumericTokenStream(AttributeSource source, int precisionStep):base(source)
		{
			InitBlock();
			this.precisionStep = precisionStep;
			if (precisionStep < 1)
				throw new System.ArgumentException("precisionStep must be >=1");
		}
		
		/// <summary> Expert: Creates a token stream for numeric values with the specified
		/// <c>precisionStep</c> using the given
		/// <see cref="Lucene.Net.Util.AttributeSource.AttributeFactory" />.
		/// The stream is not yet initialized,
		/// before using set a value using the various set<em>???</em>Value() methods.
		/// </summary>
		public NumericTokenStream(AttributeFactory factory, int precisionStep):base(factory)
		{
			InitBlock();
			this.precisionStep = precisionStep;
			if (precisionStep < 1)
				throw new System.ArgumentException("precisionStep must be >=1");
		}
		
		/// <summary> Initializes the token stream with the supplied <c>long</c> value.</summary>
		/// <param name="value_Renamed">the value, for which this TokenStream should enumerate tokens.
		/// </param>
		/// <returns> this instance, because of this you can use it the following way:
		/// <c>new Field(name, new NumericTokenStream(precisionStep).SetLongValue(value))</c>
		/// </returns>
		public NumericTokenStream SetLongValue(long value_Renamed)
		{
			this.value_Renamed = value_Renamed;
			valSize = 64;
			shift = 0;
			return this;
		}
		
		/// <summary> Initializes the token stream with the supplied <c>int</c> value.</summary>
		/// <param name="value_Renamed">the value, for which this TokenStream should enumerate tokens.
		/// </param>
		/// <returns> this instance, because of this you can use it the following way:
		/// <c>new Field(name, new NumericTokenStream(precisionStep).SetIntValue(value))</c>
		/// </returns>
		public NumericTokenStream SetIntValue(int value_Renamed)
		{
			this.value_Renamed = (long) value_Renamed;
			valSize = 32;
			shift = 0;
			return this;
		}
		
		/// <summary> Initializes the token stream with the supplied <c>double</c> value.</summary>
		/// <param name="value_Renamed">the value, for which this TokenStream should enumerate tokens.
		/// </param>
		/// <returns> this instance, because of this you can use it the following way:
		/// <c>new Field(name, new NumericTokenStream(precisionStep).SetDoubleValue(value))</c>
		/// </returns>
		public NumericTokenStream SetDoubleValue(double value_Renamed)
		{
			this.value_Renamed = NumericUtils.DoubleToSortableLong(value_Renamed);
			valSize = 64;
			shift = 0;
			return this;
		}
		
		/// <summary> Initializes the token stream with the supplied <c>float</c> value.</summary>
		/// <param name="value_Renamed">the value, for which this TokenStream should enumerate tokens.
		/// </param>
		/// <returns> this instance, because of this you can use it the following way:
		/// <c>new Field(name, new NumericTokenStream(precisionStep).SetFloatValue(value))</c>
		/// </returns>
		public NumericTokenStream SetFloatValue(float value_Renamed)
		{
			this.value_Renamed = (long) NumericUtils.FloatToSortableInt(value_Renamed);
			valSize = 32;
			shift = 0;
			return this;
		}
		
		// @Override
		public override void  Reset()
		{
			if (valSize == 0)
				throw new System.SystemException("call set???Value() before usage");
			shift = 0;
		}

        protected override void Dispose(bool disposing)
        {
            // Do nothing.
        }
		
		// @Override
		public override bool IncrementToken()
		{
			if (valSize == 0)
				goto Error;

            if (shift >= valSize)
				return false;
			
			ClearAttributes();

		    var termAttLikely = termAtt as TermAttribute;            
            if ( termAttLikely == null)
                goto Unlikely;

		    char[] buffer;
		    if (valSize == 64)
		    {
		        buffer = termAttLikely.ResizeTermBuffer(NumericUtils.BUF_SIZE_LONG);
		        termAttLikely.SetTermLength(NumericUtils.LongToPrefixCoded(value_Renamed, shift, buffer));
		    }
		    else if (valSize == 32)
		    {
		        buffer = termAttLikely.ResizeTermBuffer(NumericUtils.BUF_SIZE_INT);
		        termAttLikely.SetTermLength(NumericUtils.IntToPrefixCoded((int)value_Renamed, shift, buffer));
		    }
		    else goto Error;

		    string type = (shift == 0) ? TOKEN_TYPE_FULL_PREC : TOKEN_TYPE_LOWER_PREC;
            int increment = (shift == 0) ? 1 : 0;

		    // PERF: Try to avoid as much as possible the virtual calls here. 
		    var typeAttConcrete = typeAtt as TypeAttribute;
		    if (typeAttConcrete != null)
		        typeAttConcrete.Type = type;
		    else
		        typeAtt.Type = type;

		    // PERF: Try to avoid as much as possible the virtual calls here. 
		    var posIncrAttConcrete = posIncrAtt as PositionIncrementAttribute;
		    if (posIncrAttConcrete != null)
		        posIncrAttConcrete.PositionIncrement = increment;
		    else
		        posIncrAtt.PositionIncrement = increment;
		    
            shift += precisionStep;
		    return true;

            Unlikely:
		    return IncrementTokenUnlikely();

            Error:
		    return ThrowValueSizeIsNotValid<bool>();
        }

	    private bool IncrementTokenUnlikely()
	    {
	        char[] buffer;
	        if (valSize == 64)
	        {
	            buffer = termAtt.ResizeTermBuffer(NumericUtils.BUF_SIZE_LONG);
	            termAtt.SetTermLength(NumericUtils.LongToPrefixCoded(value_Renamed, shift, buffer));
	        }
	        else if (valSize == 32)
	        {
	            buffer = termAtt.ResizeTermBuffer(NumericUtils.BUF_SIZE_INT);
	            termAtt.SetTermLength(NumericUtils.IntToPrefixCoded((int)value_Renamed, shift, buffer));
	        }
            else return ThrowValueSizeIsNotValid<bool> ();

            typeAtt.Type = (shift == 0) ? TOKEN_TYPE_FULL_PREC : TOKEN_TYPE_LOWER_PREC;
	        posIncrAtt.PositionIncrement = (shift == 0) ? 1 : 0;
	        shift += precisionStep;
	        return true;
        }

	    private T ThrowValueSizeIsNotValid<T>()
	    {
	        if (valSize == 0)
	            throw new SystemException("called set??? Value() before usage");

	        throw new ArgumentException("valSize must be 32 or 64.");
        }

	    // @Override
		public override String ToString()
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder("(numeric,valSize=").Append(valSize);
			sb.Append(",precisionStep=").Append(precisionStep).Append(')');
			return sb.ToString();
		}
		
		// members
		private ITermAttribute termAtt;
		private ITypeAttribute typeAtt;
		private IPositionIncrementAttribute posIncrAtt;
		
		private int shift = 0, valSize = 0; // valSize==0 means not initialized
		private readonly int precisionStep;
		
		private long value_Renamed = 0L;
	}
}