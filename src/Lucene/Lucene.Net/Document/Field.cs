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
using IndexWriter = Lucene.Net.Index.IndexWriter;
using StringHelper = Lucene.Net.Util.StringHelper;

namespace Lucene.Net.Documents
{

    /// <summary>A field is a section of a Document.  Each field has two parts, a name and a
    /// value.  Values may be free text, provided as a String or as a Reader, or they
    /// may be atomic keywords, which are not further processed.  Such keywords may
    /// be used to represent dates, urls, etc.  Fields are optionally stored in the
    /// index, so that they may be returned with hits on the document.
    /// </summary>
    [Serializable]
    public sealed class Field:AbstractField, IFieldable
    {
        /// <summary>Specifies whether and how a field should be stored. </summary>
        public enum Store
        {
            /// <summary>Store the original field value in the index. This is useful for short texts
            /// like a document's title which should be displayed with the results. The
            /// value is stored in its original form, i.e. no analyzer is used before it is
            /// stored.
            /// </summary>
            YES,

            /// <summary>Do not store the field value in the index. </summary>
            NO
        }
        
        /// <summary>Specifies whether and how a field should be indexed. </summary>

        public enum Index
        {
            /// <summary>Do not index the field value. This field can thus not be searched,
            /// but one can still access its contents provided it is
            /// <see cref="Field.Store">stored</see>. 
            /// </summary>
            NO,
            
            /// <summary>Index the tokens produced by running the field's
            /// value through an Analyzer.  This is useful for
            /// common text. 
            /// </summary>
            ANALYZED,
            
            /// <summary>Index the field's value without using an Analyzer, so it can be searched.
            /// As no analyzer is used the value will be stored as a single term. This is
            /// useful for unique Ids like product numbers.
            /// </summary>
            NOT_ANALYZED,
            
            /// <summary>Expert: Index the field's value without an Analyzer,
            /// and also disable the storing of norms.  Note that you
            /// can also separately enable/disable norms by setting
            /// <see cref="AbstractField.OmitNorms" />.  No norms means that
            /// index-time field and document boosting and field
            /// length normalization are disabled.  The benefit is
            /// less memory usage as norms take up one byte of RAM
            /// per indexed field for every document in the index,
            /// during searching.  Note that once you index a given
            /// field <i>with</i> norms enabled, disabling norms will
            /// have no effect.  In other words, for this to have the
            /// above described effect on a field, all instances of
            /// that field must be indexed with NOT_ANALYZED_NO_NORMS
            /// from the beginning. 
            /// </summary>
            NOT_ANALYZED_NO_NORMS,
            
            /// <summary>Expert: Index the tokens produced by running the
            /// field's value through an Analyzer, and also
            /// separately disable the storing of norms.  See
            /// <see cref="NOT_ANALYZED_NO_NORMS" /> for what norms are
            /// and why you may want to disable them. 
            /// </summary>
            ANALYZED_NO_NORMS,
        }
        
        /// <summary>Specifies whether and how a field should have term vectors. </summary>
        public enum TermVector
        {
            /// <summary>Do not store term vectors. </summary>
            NO,
            
            /// <summary>Store the term vectors of each document. A term vector is a list
            /// of the document's terms and their number of occurrences in that document. 
            /// </summary>
            YES,
            
            /// <summary> Store the term vector + token position information
            /// 
            /// </summary>
            /// <seealso cref="YES">
            /// </seealso>
            WITH_POSITIONS,
            
            /// <summary> Store the term vector + Token offset information
            /// 
            /// </summary>
            /// <seealso cref="YES">
            /// </seealso>
            WITH_OFFSETS,
            
            /// <summary> Store the term vector + Token position and offset information
            /// 
            /// </summary>
            /// <seealso cref="YES">
            /// </seealso>
            /// <seealso cref="WITH_POSITIONS">
            /// </seealso>
            /// <seealso cref="WITH_OFFSETS">
            /// </seealso>
            WITH_POSITIONS_OFFSETS,
        }


        /// <summary>The value of the field as a String, or null.  If null, the Reader value or
        /// binary value is used.  Exactly one of stringValue(),
        /// readerValue(), and getBinaryValue() must be set. 
        /// </summary>
        public override string StringValue(IState state)
        {
            return fieldsData is System.String ? (System.String) fieldsData : null;
        }

        /// <summary>The value of the field as a Reader, or null.  If null, the String value or
        /// binary value is used.  Exactly one of stringValue(),
        /// readerValue(), and getBinaryValue() must be set. 
        /// </summary>
        public override TextReader ReaderValue
        {
            get { return fieldsData is System.IO.TextReader ? (System.IO.TextReader) fieldsData : null; }
        }

        /// <summary>The TokesStream for this field to be used when indexing, or null.  If null, the Reader value
        /// or String value is analyzed to produce the indexed tokens. 
        /// </summary>
        public override TokenStream TokenStreamValue
        {
            get { return tokenStream; }
        }


        /// <summary><p/>Expert: change the value of this field.  This can
        /// be used during indexing to re-use a single Field
        /// instance to improve indexing speed by avoiding GC cost
        /// of new'ing and reclaiming Field instances.  Typically
        /// a single <see cref="Document" /> instance is re-used as
        /// well.  This helps most on small documents.<p/>
        /// 
        /// <p/>Each Field instance should only be used once
        /// within a single <see cref="Document" /> instance.  See <a
        /// href="http://wiki.apache.org/lucene-java/ImproveIndexingSpeed">ImproveIndexingSpeed</a>
        /// for details.<p/> 
        /// </summary>
        public void  SetValue(System.String value)
        {
            if (internalIsBinary)
            {
                throw new System.ArgumentException("cannot set a String value on a binary field");
            }
            fieldsData = value;
        }
        
        /// <summary>Expert: change the value of this field.  See <a href="#setValue(java.lang.String)">setValue(String)</a>. </summary>
        public void  SetValue(System.IO.TextReader value)
        {
            if (internalIsBinary)
            {
                throw new System.ArgumentException("cannot set a Reader value on a binary field");
            }
            if (internalIsStored)
            {
                throw new System.ArgumentException("cannot set a Reader value on a stored field");
            }
            fieldsData = value;
        }
        
        /// <summary>Expert: change the value of this field.  See <a href="#setValue(java.lang.String)">setValue(String)</a>. </summary>
        public void  SetValue(byte[] value)
        {
            if (!internalIsBinary)
            {
                throw new System.ArgumentException("cannot set a byte[] value on a non-binary field");
            }
            fieldsData = value;
            internalBinaryLength = value.Length;
            internalbinaryOffset = 0;
        }
        
        /// <summary>Expert: change the value of this field.  See <a href="#setValue(java.lang.String)">setValue(String)</a>. </summary>
        public void  SetValue(byte[] value, int offset, int length)
        {
            if (!internalIsBinary)
            {
                throw new System.ArgumentException("cannot set a byte[] value on a non-binary field");
            }
            fieldsData = value;
            internalBinaryLength = length;
            internalbinaryOffset = offset;
        }
        
        /// <summary>Expert: sets the token stream to be used for indexing and causes isIndexed() and isTokenized() to return true.
        /// May be combined with stored values from stringValue() or GetBinaryValue() 
        /// </summary>
        public void  SetTokenStream(TokenStream tokenStream)
        {
            this.internalIsIndexed = true;
            this.internalIsTokenized = true;
            this.tokenStream = tokenStream;
        }
        
        /// <summary> Create a field by specifying its name, value and how it will
        /// be saved in the index. Term vectors will not be stored in the index.
        /// 
        /// </summary>
        /// <param name="name">The name of the field
        /// </param>
        /// <param name="value">The string to process
        /// </param>
        /// <param name="store">Whether <c>value</c> should be stored in the index
        /// </param>
        /// <param name="index">Whether the field should be indexed, and if so, if it should
        /// be tokenized before indexing 
        /// </param>
        /// <throws>  NullPointerException if name or value is <c>null</c> </throws>
        /// <throws>  IllegalArgumentException if the field is neither stored nor indexed  </throws>
        public Field(System.String name, System.String value, Store store, Index index)
            : this(name, value, store, index, TermVector.NO)
        {
        }
        
        /// <summary> Create a field by specifying its name, value and how it will
        /// be saved in the index.
        /// 
        /// </summary>
        /// <param name="name">The name of the field
        /// </param>
        /// <param name="value">The string to process
        /// </param>
        /// <param name="store">Whether <c>value</c> should be stored in the index
        /// </param>
        /// <param name="index">Whether the field should be indexed, and if so, if it should
        /// be tokenized before indexing 
        /// </param>
        /// <param name="termVector">Whether term vector should be stored
        /// </param>
        /// <throws>  NullPointerException if name or value is <c>null</c> </throws>
        /// <throws>  IllegalArgumentException in any of the following situations: </throws>
        /// <summary> <list> 
        /// <item>the field is neither stored nor indexed</item> 
        /// <item>the field is not indexed but termVector is <c>TermVector.YES</c></item>
        /// </list> 
        /// </summary>
        public Field(System.String name, System.String value, Store store, Index index, TermVector termVector)
            : this(name, true, value, store, index, termVector)
        {
        }
        
        /// <summary> Create a field by specifying its name, value and how it will
        /// be saved in the index.
        /// 
        /// </summary>
        /// <param name="name">The name of the field
        /// </param>
        /// <param name="internName">Whether to .intern() name or not
        /// </param>
        /// <param name="value">The string to process
        /// </param>
        /// <param name="store">Whether <c>value</c> should be stored in the index
        /// </param>
        /// <param name="index">Whether the field should be indexed, and if so, if it should
        /// be tokenized before indexing 
        /// </param>
        /// <param name="termVector">Whether term vector should be stored
        /// </param>
        /// <throws>  NullPointerException if name or value is <c>null</c> </throws>
        /// <throws>  IllegalArgumentException in any of the following situations: </throws>
        /// <summary> <list> 
        /// <item>the field is neither stored nor indexed</item> 
        /// <item>the field is not indexed but termVector is <c>TermVector.YES</c></item>
        /// </list> 
        /// </summary>
        public Field(System.String name, bool internName, System.String value, Store store, Index index, TermVector termVector)
        {
            if (name == null)
                throw new System.NullReferenceException("name cannot be null");
            if (value == null)
                throw new System.NullReferenceException("value cannot be null");
            if (name.Length == 0 && value.Length == 0)
                throw new System.ArgumentException("name and value cannot both be empty");
            if (index == Index.NO && store == Store.NO)
                throw new System.ArgumentException("it doesn't make sense to have a field that " + "is neither indexed nor stored");
            if (index == Index.NO && termVector != TermVector.NO)
                throw new System.ArgumentException("cannot store term vector information " + "for a field that is not indexed");
            
            if (internName)
            // field names are optionally interned
                name = StringHelper.Intern(name);
            
            this.internalName = name;
            
            this.fieldsData = value;

            this.internalIsStored = store.IsStored();

            this.internalIsIndexed = index.IsIndexed();
            this.internalIsTokenized = index.IsAnalyzed();
            this.internalOmitNorms = index.OmitNorms();

            if (index == Index.NO)
            {
                this.internalOmitTermFreqAndPositions = false;
            }
            
            this.internalIsBinary = false;
            
            SetStoreTermVector(termVector);
        }
        
        /// <summary> Create a tokenized and indexed field that is not stored. Term vectors will
        /// not be stored.  The Reader is read only when the Document is added to the index,
        /// i.e. you may not close the Reader until <see cref="IndexWriter.AddDocument(Document)" />
        /// has been called.
        /// 
        /// </summary>
        /// <param name="name">The name of the field
        /// </param>
        /// <param name="reader">The reader with the content
        /// </param>
        /// <throws>  NullPointerException if name or reader is <c>null</c> </throws>
        public Field(System.String name, System.IO.TextReader reader):this(name, reader, TermVector.NO)
        {
        }
        
        /// <summary> Create a tokenized and indexed field that is not stored, optionally with 
        /// storing term vectors.  The Reader is read only when the Document is added to the index,
        /// i.e. you may not close the Reader until <see cref="IndexWriter.AddDocument(Document)" />
        /// has been called.
        /// 
        /// </summary>
        /// <param name="name">The name of the field
        /// </param>
        /// <param name="reader">The reader with the content
        /// </param>
        /// <param name="termVector">Whether term vector should be stored
        /// </param>
        /// <throws>  NullPointerException if name or reader is <c>null</c> </throws>
        public Field(System.String name, System.IO.TextReader reader, TermVector termVector)
        {
            if (name == null)
                throw new System.NullReferenceException("name cannot be null");
            if (reader == null)
                throw new System.NullReferenceException("reader cannot be null");
            
            this.internalName = StringHelper.Intern(name); // field names are interned
            this.fieldsData = reader;
            
            this.internalIsStored = false;
            
            this.internalIsIndexed = true;
            this.internalIsTokenized = true;
            
            this.internalIsBinary = false;
            
            SetStoreTermVector(termVector);
        }
        
        /// <summary> Create a tokenized and indexed field that is not stored. Term vectors will
        /// not be stored. This is useful for pre-analyzed fields.
        /// The TokenStream is read only when the Document is added to the index,
        /// i.e. you may not close the TokenStream until <see cref="IndexWriter.AddDocument(Document)" />
        /// has been called.
        /// 
        /// </summary>
        /// <param name="name">The name of the field
        /// </param>
        /// <param name="tokenStream">The TokenStream with the content
        /// </param>
        /// <throws>  NullPointerException if name or tokenStream is <c>null</c> </throws>
        public Field(System.String name, TokenStream tokenStream):this(name, tokenStream, TermVector.NO)
        {
        }
        
        /// <summary> Create a tokenized and indexed field that is not stored, optionally with 
        /// storing term vectors.  This is useful for pre-analyzed fields.
        /// The TokenStream is read only when the Document is added to the index,
        /// i.e. you may not close the TokenStream until <see cref="IndexWriter.AddDocument(Document)" />
        /// has been called.
        /// 
        /// </summary>
        /// <param name="name">The name of the field
        /// </param>
        /// <param name="tokenStream">The TokenStream with the content
        /// </param>
        /// <param name="termVector">Whether term vector should be stored
        /// </param>
        /// <throws>  NullPointerException if name or tokenStream is <c>null</c> </throws>
        public Field(System.String name, TokenStream tokenStream, TermVector termVector)
        {
            if (name == null)
                throw new System.NullReferenceException("name cannot be null");
            if (tokenStream == null)
                throw new System.NullReferenceException("tokenStream cannot be null");
            
            this.internalName = StringHelper.Intern(name); // field names are interned
            this.fieldsData = null;
            this.tokenStream = tokenStream;
            
            this.internalIsStored = false;
            
            this.internalIsIndexed = true;
            this.internalIsTokenized = true;
            
            this.internalIsBinary = false;
            
            SetStoreTermVector(termVector);
        }
        
        
        /// <summary> Create a stored field with binary value. Optionally the value may be compressed.
        /// 
        /// </summary>
        /// <param name="name">The name of the field
        /// </param>
        /// <param name="value_Renamed">The binary value
        /// </param>
        /// <param name="store">How <c>value</c> should be stored (compressed or not)
        /// </param>
        /// <throws>  IllegalArgumentException if store is <c>Store.NO</c>  </throws>
        public Field(System.String name, byte[] value_Renamed, Store store):this(name, value_Renamed, 0, value_Renamed.Length, store)
        {
        }
        
        /// <summary> Create a stored field with binary value. Optionally the value may be compressed.
        /// 
        /// </summary>
        /// <param name="name">The name of the field
        /// </param>
        /// <param name="value_Renamed">The binary value
        /// </param>
        /// <param name="offset">Starting offset in value where this Field's bytes are
        /// </param>
        /// <param name="length">Number of bytes to use for this Field, starting at offset
        /// </param>
        /// <param name="store">How <c>value</c> should be stored (compressed or not)
        /// </param>
        /// <throws>  IllegalArgumentException if store is <c>Store.NO</c>  </throws>
        public Field(System.String name, byte[] value_Renamed, int offset, int length, Store store)
        {
            
            if (name == null)
                throw new System.ArgumentException("name cannot be null");
            if (value_Renamed == null)
                throw new System.ArgumentException("value cannot be null");
            
            this.internalName = StringHelper.Intern(name); // field names are interned
            fieldsData = value_Renamed;
            
            if (store == Store.NO)
                throw new System.ArgumentException("binary values can't be unstored");

            internalIsStored = store.IsStored();
            internalIsIndexed = false;
            internalIsTokenized = false;
            internalOmitTermFreqAndPositions = false;
            internalOmitNorms = true;
            
            internalIsBinary = true;
            internalBinaryLength = length;
            internalbinaryOffset = offset;
            
            SetStoreTermVector(TermVector.NO);
        }
    }

    public static class FieldExtensions
    {
        public static bool IsStored(this Field.Store store)
        {
            switch(store)
            {
                case Field.Store.YES:
                    return true;
                case Field.Store.NO:
                    return false;
                default:
                    throw new ArgumentOutOfRangeException("store", "Invalid value for Field.Store");
            }
        }

        public static bool IsIndexed(this Field.Index index)
        {
            switch(index)
            {
                case Field.Index.NO:
                    return false;
                case Field.Index.ANALYZED:
                case Field.Index.NOT_ANALYZED:
                case Field.Index.NOT_ANALYZED_NO_NORMS:
                case Field.Index.ANALYZED_NO_NORMS:
                    return true;
                default:
                    throw new ArgumentOutOfRangeException("index", "Invalid value for Field.Index");
            }
        }

        public static bool IsAnalyzed(this Field.Index index)
        {
            switch (index)
            {
                case Field.Index.NO:
                case Field.Index.NOT_ANALYZED:
                case Field.Index.NOT_ANALYZED_NO_NORMS:
                    return false;
                case Field.Index.ANALYZED:
                case Field.Index.ANALYZED_NO_NORMS:
                    return true;
                default:
                    throw new ArgumentOutOfRangeException("index", "Invalid value for Field.Index");
            }
        }

        public static bool OmitNorms(this Field.Index index)
        {
            switch (index)
            {
                case Field.Index.ANALYZED:
                case Field.Index.NOT_ANALYZED:
                    return false;
                case Field.Index.NO:
                case Field.Index.NOT_ANALYZED_NO_NORMS:
                case Field.Index.ANALYZED_NO_NORMS:
                    return true;
                default:
                    throw new ArgumentOutOfRangeException("index", "Invalid value for Field.Index");
            }
        }

        public static bool IsStored(this Field.TermVector tv)
        {
            switch(tv)
            {
                case Field.TermVector.NO:
                    return false;
                case Field.TermVector.YES:
                case Field.TermVector.WITH_OFFSETS:
                case Field.TermVector.WITH_POSITIONS:
                case Field.TermVector.WITH_POSITIONS_OFFSETS:
                    return true;
                default:
                    throw new ArgumentOutOfRangeException("tv", "Invalid value for Field.TermVector");
            }
        }

        public static bool WithPositions(this Field.TermVector tv)
        {
            switch (tv)
            {
                case Field.TermVector.NO:
                case Field.TermVector.YES:
                case Field.TermVector.WITH_OFFSETS:
                    return false;
                case Field.TermVector.WITH_POSITIONS:
                case Field.TermVector.WITH_POSITIONS_OFFSETS:
                    return true;
                default:
                    throw new ArgumentOutOfRangeException("tv", "Invalid value for Field.TermVector");
            }
        }

        public static bool WithOffsets(this Field.TermVector tv)
        {
            switch (tv)
            {
                case Field.TermVector.NO:
                case Field.TermVector.YES:
                case Field.TermVector.WITH_POSITIONS:
                    return false;
                case Field.TermVector.WITH_OFFSETS:
                case Field.TermVector.WITH_POSITIONS_OFFSETS:
                    return true;
                default:
                    throw new ArgumentOutOfRangeException("tv", "Invalid value for Field.TermVector");
            }
        }

        public static Field.Index ToIndex(bool indexed, bool analyed)
        {
            return ToIndex(indexed, analyed, false);
        }

        public static Field.Index ToIndex(bool indexed, bool analyzed, bool omitNorms)
        {

            // If it is not indexed nothing else matters
            if (!indexed)
            {
                return Field.Index.NO;
            }

            // typical, non-expert
            if (!omitNorms)
            {
                if (analyzed)
                {
                    return Field.Index.ANALYZED;
                }
                return Field.Index.NOT_ANALYZED;
            }

            // Expert: Norms omitted
            if (analyzed)
            {
                return Field.Index.ANALYZED_NO_NORMS;
            }
            return Field.Index.NOT_ANALYZED_NO_NORMS;
        }

        /// <summary>
        /// Get the best representation of a TermVector given the flags.
        /// </summary>
        public static Field.TermVector ToTermVector(bool stored, bool withOffsets, bool withPositions)
        {
            // If it is not stored, nothing else matters.
            if (!stored)
            {
                return Field.TermVector.NO;
            }

            if (withOffsets)
            {
                if (withPositions)
                {
                    return Field.TermVector.WITH_POSITIONS_OFFSETS;
                }
                return Field.TermVector.WITH_OFFSETS;
            }

            if (withPositions)
            {
                return Field.TermVector.WITH_POSITIONS;
            }
            return Field.TermVector.YES;
        }
    }
}