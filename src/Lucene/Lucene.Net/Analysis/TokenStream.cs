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
using Lucene.Net.Util;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using AttributeSource = Lucene.Net.Util.AttributeSource;

namespace Lucene.Net.Analysis
{
	
	/// <summary> A <c>TokenStream</c> enumerates the sequence of tokens, either from
	/// <see cref="Field" />s of a <see cref="Document" /> or from query text.
	/// <p/>
	/// This is an abstract class. Concrete subclasses are:
	/// <list type="bullet">
	/// <item><see cref="Tokenizer" />, a <c>TokenStream</c> whose input is a Reader; and</item>
	/// <item><see cref="TokenFilter" />, a <c>TokenStream</c> whose input is another
	/// <c>TokenStream</c>.</item>
	/// </list>
	/// A new <c>TokenStream</c> API has been introduced with Lucene 2.9. This API
	/// has moved from being <see cref="Token" /> based to <see cref="IAttribute" /> based. While
	/// <see cref="Token" /> still exists in 2.9 as a convenience class, the preferred way
	/// to store the information of a <see cref="Token" /> is to use <see cref="Util.Attribute" />s.
	/// <p/>
	/// <c>TokenStream</c> now extends <see cref="AttributeSource" />, which provides
	/// access to all of the token <see cref="IAttribute" />s for the <c>TokenStream</c>.
	/// Note that only one instance per <see cref="Util.Attribute" /> is created and reused
	/// for every token. This approach reduces object creation and allows local
	/// caching of references to the <see cref="Util.Attribute" />s. See
	/// <see cref="IncrementToken()" /> for further details.
	/// <p/>
	/// <b>The workflow of the new <c>TokenStream</c> API is as follows:</b>
	/// <list type="bullet">
	/// <item>Instantiation of <c>TokenStream</c>/<see cref="TokenFilter" />s which add/get
	/// attributes to/from the <see cref="AttributeSource" />.</item>
	/// <item>The consumer calls <see cref="TokenStream.Reset()" />.</item>
	/// <item>The consumer retrieves attributes from the stream and stores local
	/// references to all attributes it wants to access</item>
	/// <item>The consumer calls <see cref="IncrementToken()" /> until it returns false and
	/// consumes the attributes after each call.</item>
	/// <item>The consumer calls <see cref="End()" /> so that any end-of-stream operations
	/// can be performed.</item>
	/// <item>The consumer calls <see cref="Close()" /> to release any resource when finished
	/// using the <c>TokenStream</c></item>
	/// </list>
	/// To make sure that filters and consumers know which attributes are available,
	/// the attributes must be added during instantiation. Filters and consumers are
	/// not required to check for availability of attributes in
	/// <see cref="IncrementToken()" />.
	/// <p/>
	/// You can find some example code for the new API in the analysis package level
	/// Javadoc.
	/// <p/>
	/// Sometimes it is desirable to capture a current state of a <c>TokenStream</c>
	/// , e. g. for buffering purposes (see <see cref="CachingTokenFilter" />,
	/// <see cref="TeeSinkTokenFilter" />). For this usecase
	/// <see cref="AttributeSource.CaptureState" /> and <see cref="AttributeSource.RestoreState" />
	/// can be used.
	/// </summary>
	public abstract class TokenStream : AttributeSource, IDisposable
	{
		/// <summary> A TokenStream using the default attribute factory.</summary>
		protected internal TokenStream()
		{ }
		
		/// <summary> A TokenStream that uses the same attributes as the supplied one.</summary>
        protected internal TokenStream(AttributeSource input)
            : base(input)
		{ }
		
		/// <summary> A TokenStream using the supplied AttributeFactory for creating new <see cref="IAttribute" /> instances.</summary>
        protected internal TokenStream(AttributeFactory factory)
            : base(factory)
		{ }

	    /// <summary> Consumers (i.e., <see cref="IndexWriter" />) use this method to advance the stream to
	    /// the next token. Implementing classes must implement this method and update
	    /// the appropriate <see cref="Util.Attribute" />s with the attributes of the next
	    /// token.
	    /// 
	    /// The producer must make no assumptions about the attributes after the
	    /// method has been returned: the caller may arbitrarily change it. If the
	    /// producer needs to preserve the state for subsequent calls, it can use
	    /// <see cref="AttributeSource.CaptureState" /> to create a copy of the current attribute state.
	    /// 
	    /// This method is called for every token of a document, so an efficient
	    /// implementation is crucial for good performance. To avoid calls to
	    /// <see cref="AttributeSource.AddAttribute{T}()" /> and <see cref="AttributeSource.GetAttribute{T}()" />,
	    /// references to all <see cref="Util.Attribute" />s that this stream uses should be
	    /// retrieved during instantiation.
	    /// 
	    /// To ensure that filters and consumers know which attributes are available,
	    /// the attributes must be added during instantiation. Filters and consumers
	    /// are not required to check for availability of attributes in
	    /// <see cref="IncrementToken()" />.
	    /// 
	    /// </summary>
	    /// <returns> false for end of stream; true otherwise</returns>
	    public abstract bool IncrementToken();
		
		/// <summary> This method is called by the consumer after the last token has been
		/// consumed, after <see cref="IncrementToken" /> returned <c>false</c>
		/// (using the new <c>TokenStream</c> API). Streams implementing the old API
		/// should upgrade to use this feature.
		/// <p/>
		/// This method can be used to perform any end-of-stream operations, such as
		/// setting the final offset of a stream. The final offset of a stream might
		/// differ from the offset of the last token eg in case one or more whitespaces
		/// followed after the last token, but a <see cref="WhitespaceTokenizer" /> was used.
		/// 
		/// </summary>
		/// <throws>  IOException </throws>
		public virtual void  End()
		{
			// do nothing by default
		}
		
		/// <summary> Resets this stream to the beginning. This is an optional operation, so
		/// subclasses may or may not implement this method. <see cref="Reset()" /> is not needed for
		/// the standard indexing process. However, if the tokens of a
		/// <c>TokenStream</c> are intended to be consumed more than once, it is
		/// necessary to implement <see cref="Reset()" />. Note that if your TokenStream
		/// caches tokens and feeds them back again after a reset, it is imperative
		/// that you clone the tokens when you store them away (on the first pass) as
		/// well as when you return them (on future passes after <see cref="Reset()" />).
		/// </summary>
		public virtual void  Reset()
		{
		}
		
		/// <summary>Releases resources associated with this stream. </summary>
		[Obsolete("Use Dispose() instead")]
		public void  Close()
		{
            Dispose();
		}

        public void Dispose()
        {
            Dispose(true);
        }

	    protected abstract void Dispose(bool disposing);
	}
}