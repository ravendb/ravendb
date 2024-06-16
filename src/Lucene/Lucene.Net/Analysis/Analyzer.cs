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
using Lucene.Net.Documents;
using Lucene.Net.Store;
using Lucene.Net.Util;

namespace Lucene.Net.Analysis
{
	/// <summary>An Analyzer builds TokenStreams, which analyze text.  It thus represents a
	/// policy for extracting index terms from text.
	/// <p/>
	/// Typical implementations first build a Tokenizer, which breaks the stream of
	/// characters from the Reader into raw Tokens.  One or more TokenFilters may
	/// then be applied to the output of the Tokenizer.
	/// </summary>
	public abstract class Analyzer : IDisposable
	{
		/// <summary>Creates a TokenStream which tokenizes all the text in the provided
		/// Reader.  Must be able to handle null field name for
		/// backward compatibility.
		/// </summary>
		public abstract TokenStream TokenStream(String fieldName, System.IO.TextReader reader);
		
		/// <summary>Creates a TokenStream that is allowed to be re-used
		/// from the previous time that the same thread called
		/// this method.  Callers that do not need to use more
		/// than one TokenStream at the same time from this
		/// analyzer should use this method for better
		/// performance.
		/// </summary>
		public virtual TokenStream ReusableTokenStream(String fieldName, System.IO.TextReader reader)
		{
			return TokenStream(fieldName, reader);
		}
		
		private LightWeightThreadLocal<Object> tokenStreams = new LightWeightThreadLocal<Object>();
	    private bool isDisposed;

	    /// <summary>Used by Analyzers that implement reusableTokenStream
	    /// to retrieve previously saved TokenStreams for re-use
	    /// by the same thread. 
	    /// </summary>
	    protected internal virtual object PreviousTokenStream
	    {
	        get
	        {
	            if (tokenStreams == null)
	            {
	                throw new AlreadyClosedException("this Analyzer is closed");
	            }
	            return tokenStreams.Get(StateHolder.Current.Value);
	        }
	        set
	        {
	            if (tokenStreams == null)
	            {
	                throw new AlreadyClosedException("this Analyzer is closed");
	            }
	            tokenStreams.Set(value);
	        }
	    }

	    [Obsolete()]
		protected internal bool overridesTokenStreamMethod = false;
		
		/// <deprecated> This is only present to preserve
		/// back-compat of classes that subclass a core analyzer
		/// and override tokenStream but not reusableTokenStream 
		/// </deprecated>
		/// <summary>
        /// Java uses Class&lt;? extends Analyer&gt; to constrain <typeparamref name="TClass"/> to
        /// only Types that inherit from Analyzer.  C# does not have a generic type class,
        /// ie Type&lt;t&gt;.  The method signature stays the same, and an exception may
        /// still be thrown, if the method doesn't exist.
		/// </summary>
        [Obsolete("This is only present to preserve back-compat of classes that subclass a core analyzer and override tokenStream but not reusableTokenStream ")]
		protected internal virtual void SetOverridesTokenStreamMethod<TClass>()
            where TClass : Analyzer
		{
            try
            {
                System.Reflection.MethodInfo m = this.GetType().GetMethod("TokenStream", new[] { typeof(string), typeof(System.IO.TextReader) });
                overridesTokenStreamMethod = m.DeclaringType != typeof(TClass);
            }
            catch (MethodAccessException)
            {
                // can't happen, as baseClass is subclass of Analyzer
                overridesTokenStreamMethod = false;
            }
		}
		
		
		/// <summary> Invoked before indexing a Fieldable instance if
		/// terms have already been added to that field.  This allows custom
		/// analyzers to place an automatic position increment gap between
		/// Fieldable instances using the same field name.  The default value
		/// position increment gap is 0.  With a 0 position increment gap and
		/// the typical default token position increment of 1, all terms in a field,
		/// including across Fieldable instances, are in successive positions, allowing
		/// exact PhraseQuery matches, for instance, across Fieldable instance boundaries.
		/// 
		/// </summary>
		/// <param name="fieldName">Fieldable name being indexed.
		/// </param>
		/// <returns> position increment gap, added to the next token emitted from <see cref="TokenStream(String,System.IO.TextReader)" />
		/// </returns>
		public virtual int GetPositionIncrementGap(String fieldName)
		{
			return 0;
		}
		
		/// <summary> Just like <see cref="GetPositionIncrementGap" />, except for
		/// Token offsets instead.  By default this returns 1 for
		/// tokenized fields and, as if the fields were joined
		/// with an extra space character, and 0 for un-tokenized
		/// fields.  This method is only called if the field
		/// produced at least one token for indexing.
		/// 
		/// </summary>
		/// <param name="field">the field just indexed
		/// </param>
		/// <returns> offset gap, added to the next token emitted from <see cref="TokenStream(String,System.IO.TextReader)" />
		/// </returns>
		public virtual int GetOffsetGap(IFieldable field)
		{
			return field.IsTokenized ? 1 : 0;
		}

		/// <summary>Frees persistent resources used by this Analyzer </summary>
		public void  Close()
		{
		    Dispose();
		}

        public virtual void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (isDisposed) return;

            if (disposing)
            {
                if (tokenStreams != null)
                {
                    tokenStreams.Dispose();
                    tokenStreams = null;
                }
            }
            isDisposed = true;
        }
	}
}