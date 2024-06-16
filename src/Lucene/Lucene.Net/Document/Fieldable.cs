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
using FieldInvertState = Lucene.Net.Index.FieldInvertState;

namespace Lucene.Net.Documents
{
	/// <summary> Synonymous with <see cref="Field" />.
	/// 
	/// <p/><bold>WARNING</bold>: This interface may change within minor versions, despite Lucene's backward compatibility requirements.
	/// This means new methods may be added from version to version.  This change only affects the Fieldable API; other backwards
	/// compatibility promises remain intact. For example, Lucene can still
	/// read and write indices created within the same major version.
	/// <p/>
	/// 
	/// 
	/// </summary>
	public interface IFieldable
	{
        /// <summary>Gets or sets the boost factor for hits for this field.  This value will be
        /// multiplied into the score of all hits on this this field of this
        /// document.
	    /// 
	    /// <p/>The boost is multiplied by <see cref="Lucene.Net.Documents.Document.Boost" /> of the document
	    /// containing this field.  If a document has multiple fields with the same
	    /// name, all such values are multiplied together.  This product is then
	    /// used to compute the norm factor for the field.  By
	    /// default, in the <see cref="Lucene.Net.Search.Similarity.ComputeNorm(String,Lucene.Net.Index.FieldInvertState)"/>
	    /// method, the boost value is multiplied
	    /// by the <see cref="Lucene.Net.Search.Similarity.LengthNorm(String,int)"/>
	    /// and then rounded by <see cref="Lucene.Net.Search.Similarity.EncodeNorm(float)" /> before it is stored in the
	    /// index.  One should attempt to ensure that this product does not overflow
	    /// the range of that encoding.
        /// 
        /// <p/>The default value is 1.0.
        /// 
        /// <p/>Note: this value is not stored directly with the document in the index.
        /// Documents returned from <see cref="Document" /> and
        /// <see cref="Lucene.Net.Search.Searcher.Doc(int)" /> may thus not have the same value present as when
        /// this field was indexed.
        /// 
	    /// </summary>
	    /// <seealso cref="Lucene.Net.Documents.Document.Boost">
	    /// </seealso>
	    /// <seealso cref="Lucene.Net.Search.Similarity.ComputeNorm(String, FieldInvertState)">
	    /// </seealso>
	    /// <seealso cref="Lucene.Net.Search.Similarity.EncodeNorm(float)">
	    /// </seealso>
	    float Boost { get; set; }

	    /// <summary>Returns the name of the field as an interned string.
	    /// For example "date", "title", "body", ...
	    /// </summary>
	    string Name { get; }

	    /// <summary>The value of the field as a String, or null.
	    /// <p/>
	    /// For indexing, if isStored()==true, the stringValue() will be used as the stored field value
	    /// unless isBinary()==true, in which case GetBinaryValue() will be used.
	    /// 
	    /// If isIndexed()==true and isTokenized()==false, this String value will be indexed as a single token.
	    /// If isIndexed()==true and isTokenized()==true, then tokenStreamValue() will be used to generate indexed tokens if not null,
	    /// else readerValue() will be used to generate indexed tokens if not null, else stringValue() will be used to generate tokens.
	    /// </summary>
	    string StringValue(IState state);

	    /// <summary>The value of the field as a Reader, which can be used at index time to generate indexed tokens.</summary>
	    /// <seealso cref="StringValue()">
	    /// </seealso>
	    TextReader ReaderValue { get; }

	    /// <summary>The TokenStream for this field to be used when indexing, or null.</summary>
	    /// <seealso cref="StringValue()">
	    /// </seealso>
	    TokenStream TokenStreamValue { get; }

	    /// <summary>True if the value of the field is to be stored in the index for return
	    /// with search hits. 
	    /// </summary>
	    bool IsStored { get; }

	    /// <summary>True if the value of the field is to be indexed, so that it may be
	    /// searched on. 
	    /// </summary>
	    bool IsIndexed { get; }

	    /// <summary>True if the value of the field should be tokenized as text prior to
	    /// indexing.  Un-tokenized fields are indexed as a single word and may not be
	    /// Reader-valued. 
	    /// </summary>
	    bool IsTokenized { get; }

	    /// <summary>True if the term or terms used to index this field are stored as a term
	    /// vector, available from <see cref="Lucene.Net.Index.IndexReader.GetTermFreqVector(int,String)" />.
	    /// These methods do not provide access to the original content of the field,
	    /// only to terms used to index it. If the original content must be
	    /// preserved, use the <c>stored</c> attribute instead.
	    /// 
	    /// </summary>
	    /// <seealso cref="Lucene.Net.Index.IndexReader.GetTermFreqVector(int, String)">
	    /// </seealso>
	    bool IsTermVectorStored { get; }

	    /// <summary> True if terms are stored as term vector together with their offsets 
	    /// (start and end positon in source text).
	    /// </summary>
	    bool IsStoreOffsetWithTermVector { get; }

	    /// <summary> True if terms are stored as term vector together with their token positions.</summary>
	    bool IsStorePositionWithTermVector { get; }

	    /// <summary>True if the value of the field is stored as binary </summary>
	    bool IsBinary { get; }

        /// <summary>
        /// True if norms are omitted for this indexed field.
        /// <para>
        /// Expert:
        /// If set, omit normalization factors associated with this indexed field.
        /// This effectively disables indexing boosts and length normalization for this field.
        /// </para>
        /// </summary>
	    bool OmitNorms { get; set; }


	    /// <summary> Indicates whether a Field is Lazy or not.  The semantics of Lazy loading are such that if a Field is lazily loaded, retrieving
	    /// it's values via <see cref="StringValue()" /> or <see cref="GetBinaryValue()" /> is only valid as long as the <see cref="Lucene.Net.Index.IndexReader" /> that
	    /// retrieved the <see cref="Document" /> is still open.
	    /// 
	    /// </summary>
	    /// <value> true if this field can be loaded lazily </value>
	    bool IsLazy { get; }

	    /// <summary> Returns offset into byte[] segment that is used as value, if Field is not binary
	    /// returned value is undefined
	    /// </summary>
	    /// <value> index of the first character in byte[] segment that represents this Field value </value>
	    int BinaryOffset { get; }

	    /// <summary> Returns length of byte[] segment that is used as value, if Field is not binary
	    /// returned value is undefined
	    /// </summary>
	    /// <value> length of byte[] segment that represents this Field value </value>
	    int BinaryLength { get; }

	    /// <summary> Return the raw byte[] for the binary field.  Note that
	    /// you must also call <see cref="BinaryLength" /> and <see cref="BinaryOffset" />
	    /// to know which range of bytes in this
	    /// returned array belong to the field.
	    /// </summary>
	    /// <returns> reference to the Field value as byte[]. </returns>
	    byte[] GetBinaryValue(IState state);

	    /// <summary> Return the raw byte[] for the binary field.  Note that
        /// you must also call <see cref="BinaryLength" /> and <see cref="BinaryOffset" />
		/// to know which range of bytes in this
		/// returned array belong to the field.<p/>
		/// About reuse: if you pass in the result byte[] and it is
		/// used, likely the underlying implementation will hold
		/// onto this byte[] and return it in future calls to
		/// <see cref="GetBinaryValue()" /> or <see cref="GetBinaryValue()" />.
		/// So if you subsequently re-use the same byte[] elsewhere
		/// it will alter this Fieldable's value.
		/// </summary>
		/// <param name="result"> User defined buffer that will be used if
		/// possible.  If this is null or not large enough, a new
		/// buffer is allocated
		/// </param>
		/// <returns> reference to the Field value as byte[].
		/// </returns>
		byte[] GetBinaryValue(byte[] result, IState state);

	    /// Expert:
	    /// <para>
	    /// If set, omit term freq, positions and payloads from
	    /// postings for this field.
	    /// </para>
	    /// <para>
	    /// <b>NOTE</b>: While this option reduces storage space
	    /// required in the index, it also means any query
	    /// requiring positional information, such as
	    /// <see cref="Lucene.Net.Search.PhraseQuery"/> or 
	    /// <see cref="Lucene.Net.Search.Spans.SpanQuery"/> 
	    /// subclasses will silently fail to find results.
	    /// </para>
	    bool OmitTermFreqAndPositions { set; get; }
	}
}