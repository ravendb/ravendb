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
using Lucene.Net.Store;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using StringHelper = Lucene.Net.Util.StringHelper;
using PhraseQuery = Lucene.Net.Search.PhraseQuery;
using SpanQuery = Lucene.Net.Search.Spans.SpanQuery;

namespace Lucene.Net.Documents
{
    /// <summary> 
    /// 
    /// 
    /// </summary>
    [Serializable]
    public abstract class AbstractField : IFieldable
	{
		
		protected internal System.String internalName = "body";
		protected internal bool storeTermVector = false;
		protected internal bool storeOffsetWithTermVector = false;
		protected internal bool storePositionWithTermVector = false;
		protected internal bool internalOmitNorms = false;
		protected internal bool internalIsStored = false;
		protected internal bool internalIsIndexed = true;
		protected internal bool internalIsTokenized = true;
		protected internal bool internalIsBinary = false;
		protected internal bool lazy = false;
		protected internal bool internalOmitTermFreqAndPositions = false;
		protected internal float internalBoost = 1.0f;
		// the data object for all different kind of field values
		protected internal System.Object fieldsData = null;
		// pre-analyzed tokenStream for indexed fields
		protected internal TokenStream tokenStream;
		// length/offset for all primitive types
		protected internal int internalBinaryLength;
		protected internal int internalbinaryOffset;
		
		protected internal AbstractField()
		{
		}
		
		protected internal AbstractField(System.String name, Field.Store store, Field.Index index, Field.TermVector termVector)
		{
			if (name == null)
				throw new System.NullReferenceException("name cannot be null");
			this.internalName = StringHelper.Intern(name); // field names are interned

		    this.internalIsStored = store.IsStored();
		    this.internalIsIndexed = index.IsIndexed();
		    this.internalIsTokenized = index.IsAnalyzed();
		    this.internalOmitNorms = index.OmitNorms();
			
			this.internalIsBinary = false;
			
			SetStoreTermVector(termVector);
		}

	    /// <summary>Gets or sets the boost factor for hits for this field.
	    /// 
	    /// <p/>The default value is 1.0.
	    /// 
	    /// <p/>Note: this value is not stored directly with the document in the index.
	    /// Documents returned from <see cref="Lucene.Net.Index.IndexReader.Document(int)" /> and
	    /// <see cref="Lucene.Net.Search.Searcher.Doc(int)" /> may thus not have the same value present as when
	    /// this field was indexed.
	    /// </summary>
	    public virtual float Boost
	    {
	        get { return internalBoost; }
	        set { this.internalBoost = value; }
	    }

	    /// <summary>Returns the name of the field as an interned string.
	    /// For example "date", "title", "body", ...
	    /// </summary>
	    public virtual string Name
	    {
	        get { return internalName; }
	    }

	    protected internal virtual void  SetStoreTermVector(Field.TermVector termVector)
		{
		    this.storeTermVector = termVector.IsStored();
		    this.storePositionWithTermVector = termVector.WithPositions();
		    this.storeOffsetWithTermVector = termVector.WithOffsets();
		}

	    /// <summary>True iff the value of the field is to be stored in the index for return
	    /// with search hits.  It is an error for this to be true if a field is
	    /// Reader-valued. 
	    /// </summary>
	    public bool IsStored
	    {
	        get { return internalIsStored; }
	    }

	    /// <summary>True iff the value of the field is to be indexed, so that it may be
	    /// searched on. 
	    /// </summary>
	    public bool IsIndexed
	    {
	        get { return internalIsIndexed; }
	    }

	    /// <summary>True iff the value of the field should be tokenized as text prior to
	    /// indexing.  Un-tokenized fields are indexed as a single word and may not be
	    /// Reader-valued. 
	    /// </summary>
	    public bool IsTokenized
	    {
	        get { return internalIsTokenized; }
	    }

	    /// <summary>True iff the term or terms used to index this field are stored as a term
	    /// vector, available from <see cref="Lucene.Net.Index.IndexReader.GetTermFreqVector(int,String)" />.
	    /// These methods do not provide access to the original content of the field,
	    /// only to terms used to index it. If the original content must be
	    /// preserved, use the <c>stored</c> attribute instead.
	    /// 
	    /// </summary>
	    /// <seealso cref="Lucene.Net.Index.IndexReader.GetTermFreqVector(int, String)">
	    /// </seealso>
	    public bool IsTermVectorStored
	    {
	        get { return storeTermVector; }
	    }

	    /// <summary> True iff terms are stored as term vector together with their offsets 
	    /// (start and end position in source text).
	    /// </summary>
	    public virtual bool IsStoreOffsetWithTermVector
	    {
	        get { return storeOffsetWithTermVector; }
	    }

	    /// <summary> True iff terms are stored as term vector together with their token positions.</summary>
	    public virtual bool IsStorePositionWithTermVector
	    {
	        get { return storePositionWithTermVector; }
	    }

	    /// <summary>True iff the value of the filed is stored as binary </summary>
	    public bool IsBinary
	    {
	        get { return internalIsBinary; }
	    }


	    /// <summary> Return the raw byte[] for the binary field.  Note that
	    /// you must also call <see cref="BinaryLength" /> and <see cref="BinaryOffset" />
	    /// to know which range of bytes in this
	    /// returned array belong to the field.
	    /// </summary>
	    /// <returns> reference to the Field value as byte[]. </returns>
	    public virtual byte[] GetBinaryValue(IState state)
	    {
	        return GetBinaryValue(null, state);
	    }

	    public virtual byte[] GetBinaryValue(byte[] result, IState state)
		{
			if (internalIsBinary || fieldsData is byte[])
				return (byte[]) fieldsData;
			else
				return null;
		}

	    /// <summary> Returns length of byte[] segment that is used as value, if Field is not binary
	    /// returned value is undefined
	    /// </summary>
	    /// <value> length of byte[] segment that represents this Field value </value>
	    public virtual int BinaryLength
	    {
	        get
	        {
	            if (internalIsBinary)
	            {
	                return internalBinaryLength;
	            }
	            return fieldsData is byte[] ? ((byte[]) fieldsData).Length : 0;
	        }
	    }

	    /// <summary> Returns offset into byte[] segment that is used as value, if Field is not binary
	    /// returned value is undefined
	    /// </summary>
	    /// <value> index of the first character in byte[] segment that represents this Field value </value>
	    public virtual int BinaryOffset
	    {
	        get { return internalbinaryOffset; }
	    }

	    /// <summary>True if norms are omitted for this indexed field </summary>
	    public virtual bool OmitNorms
	    {
	        get { return internalOmitNorms; }
	        set { this.internalOmitNorms = value; }
	    }

	    /// <summary>Expert:
	    /// 
	    /// If set, omit term freq, positions and payloads from
	    /// postings for this field.
	    /// 
	    /// <p/><b>NOTE</b>: While this option reduces storage space
	    /// required in the index, it also means any query
	    /// requiring positional information, such as <see cref="PhraseQuery" />
	    /// or <see cref="SpanQuery" /> subclasses will
	    /// silently fail to find results.
	    /// </summary>
	    public virtual bool OmitTermFreqAndPositions
	    {
	        set { this.internalOmitTermFreqAndPositions = value; }
	        get { return internalOmitTermFreqAndPositions; }
	    }

	    public virtual bool IsLazy
	    {
	        get { return lazy; }
	    }

	    /// <summary>Prints a Field for human consumption. </summary>
		public override System.String ToString()
		{
			System.Text.StringBuilder result = new System.Text.StringBuilder();
			if (internalIsStored)
			{
				result.Append("stored");
			}
			if (internalIsIndexed)
			{
				if (result.Length > 0)
					result.Append(",");
				result.Append("indexed");
			}
			if (internalIsTokenized)
			{
				if (result.Length > 0)
					result.Append(",");
				result.Append("tokenized");
			}
			if (storeTermVector)
			{
				if (result.Length > 0)
					result.Append(",");
				result.Append("termVector");
			}
			if (storeOffsetWithTermVector)
			{
				if (result.Length > 0)
					result.Append(",");
				result.Append("termVectorOffsets");
			}
			if (storePositionWithTermVector)
			{
				if (result.Length > 0)
					result.Append(",");
				result.Append("termVectorPosition");
			}
			if (internalIsBinary)
			{
				if (result.Length > 0)
					result.Append(",");
				result.Append("binary");
			}
			if (internalOmitNorms)
			{
				result.Append(",omitNorms");
			}
			if (internalOmitTermFreqAndPositions)
			{
				result.Append(",omitTermFreqAndPositions");
			}
			if (lazy)
			{
				result.Append(",lazy");
			}
			result.Append('<');
			result.Append(internalName);
			result.Append(':');
			
			if (fieldsData != null && lazy == false)
			{
				result.Append(fieldsData);
			}
			
			result.Append('>');
			return result.ToString();
		}

	    public abstract TokenStream TokenStreamValue { get; }
	    public abstract TextReader ReaderValue { get; }
	    public abstract string StringValue(IState state);
	}
}