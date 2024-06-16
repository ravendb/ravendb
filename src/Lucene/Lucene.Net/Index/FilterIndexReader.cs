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
using Lucene.Net.Store;
using Document = Lucene.Net.Documents.Document;
using FieldSelector = Lucene.Net.Documents.FieldSelector;
using Directory = Lucene.Net.Store.Directory;

namespace Lucene.Net.Index
{
	
	/// <summary>A <c>FilterIndexReader</c> contains another IndexReader, which it
	/// uses as its basic source of data, possibly transforming the data along the
	/// way or providing additional functionality. The class
	/// <c>FilterIndexReader</c> itself simply implements all abstract methods
	/// of <c>IndexReader</c> with versions that pass all requests to the
	/// contained index reader. Subclasses of <c>FilterIndexReader</c> may
	/// further override some of these methods and may also provide additional
	/// methods and fields.
	/// </summary>
	public class FilterIndexReader:IndexReader
	{

        /// <summary>Base class for filtering <see cref="Lucene.Net.Index.TermDocs" /> implementations. </summary>
		public class FilterTermDocs : TermDocs
		{
			protected internal TermDocs in_Renamed;
			
			public FilterTermDocs(TermDocs in_Renamed)
			{
				this.in_Renamed = in_Renamed;
			}
			
			public virtual void  Seek(Term term, IState state)
			{
				in_Renamed.Seek(term, state);
			}
			public virtual void  Seek(TermEnum termEnum, IState state)
			{
				in_Renamed.Seek(termEnum, state);
			}

            public virtual int Doc
            {
                get { return in_Renamed.Doc; }
            }

            public virtual int Freq
            {
                get { return in_Renamed.Freq; }
            }

            public virtual bool Next(IState state)
			{
				return in_Renamed.Next(state);
			}
			public virtual int Read(Span<int> docs, Span<int> freqs, IState state)
			{
				return in_Renamed.Read(docs, freqs, state);
			}
			public virtual bool SkipTo(int i, IState state)
			{
				return in_Renamed.SkipTo(i, state);
			}

			public void Close()
			{
				Dispose();
			}

            public void Dispose()
            {
                Dispose(true);
            }

            protected virtual void Dispose(bool disposing)
            {
                if (disposing)
                {
                    in_Renamed.Close();
                }
            }
		}
		
		/// <summary>Base class for filtering <see cref="TermPositions" /> implementations. </summary>
		public class FilterTermPositions:FilterTermDocs, TermPositions
		{
			
			public FilterTermPositions(TermPositions in_Renamed):base(in_Renamed)
			{
			}
			
			public virtual int NextPosition(IState state)
			{
				return ((TermPositions) this.in_Renamed).NextPosition(state);
			}

		    public virtual int PayloadLength
		    {
		        get { return ((TermPositions) this.in_Renamed).PayloadLength; }
		    }

		    public virtual byte[] GetPayload(byte[] data, int offset, IState state)
			{
				return ((TermPositions) this.in_Renamed).GetPayload(data, offset, state);
			}
			
			
			// TODO: Remove warning after API has been finalized

		    public virtual bool IsPayloadAvailable
		    {
		        get { return ((TermPositions) this.in_Renamed).IsPayloadAvailable; }
		    }
		}
		
		/// <summary>Base class for filtering <see cref="TermEnum" /> implementations. </summary>
		public class FilterTermEnum:TermEnum
		{
			protected internal TermEnum in_Renamed;
			
			public FilterTermEnum(TermEnum in_Renamed)
			{
				this.in_Renamed = in_Renamed;
			}
			
			public override bool Next(IState state)
			{
				return in_Renamed.Next(state);
			}

		    public override Term Term
		    {
		        get { return in_Renamed.Term; }
		    }

		    public override int DocFreq()
			{
				return in_Renamed.DocFreq();
			}

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    in_Renamed.Close();
                }
            }
		}
		
		protected internal IndexReader in_Renamed;
		
		/// <summary> <p/>Construct a FilterIndexReader based on the specified base reader.
		/// Directory locking for delete, undeleteAll, and setNorm operations is
		/// left to the base reader.<p/>
		/// <p/>Note that base reader is closed if this FilterIndexReader is closed.<p/>
		/// </summary>
		///  <param name="in_Renamed">specified base reader.
		/// </param>
		public FilterIndexReader(IndexReader in_Renamed):base()
		{
			this.in_Renamed = in_Renamed;
		}
		
		public override Directory Directory()
		{
			return in_Renamed.Directory();
		}
		
		public override ITermFreqVector[] GetTermFreqVectors(int docNumber, IState state)
		{
			EnsureOpen();
			return in_Renamed.GetTermFreqVectors(docNumber, state);
		}
		
		public override ITermFreqVector GetTermFreqVector(int docNumber, System.String field, IState state)
		{
			EnsureOpen();
			return in_Renamed.GetTermFreqVector(docNumber, field, state);
		}
		
		
		public override void  GetTermFreqVector(int docNumber, System.String field, TermVectorMapper mapper, IState state)
		{
			EnsureOpen();
			in_Renamed.GetTermFreqVector(docNumber, field, mapper, state);
		}
		
		public override void  GetTermFreqVector(int docNumber, TermVectorMapper mapper, IState state)
		{
			EnsureOpen();
			in_Renamed.GetTermFreqVector(docNumber, mapper, state);
		}

	    public override int NumDocs()
	    {
	        // Don't call ensureOpen() here (it could affect performance)
	        return in_Renamed.NumDocs();
	    }

	    public override int MaxDoc
	    {
	        get
	        {
	            // Don't call ensureOpen() here (it could affect performance)
	            return in_Renamed.MaxDoc;
	        }
	    }

	    public override Document Document(int n, FieldSelector fieldSelector, IState state)
		{
			EnsureOpen();
			return in_Renamed.Document(n, fieldSelector, state);
		}
		
		public override bool IsDeleted(int n)
		{
			// Don't call ensureOpen() here (it could affect performance)
			return in_Renamed.IsDeleted(n);
		}

	    public override bool HasDeletions
	    {
	        get
	        {
	            // Don't call ensureOpen() here (it could affect performance)
	            return in_Renamed.HasDeletions;
	        }
	    }

	    protected internal override void  DoUndeleteAll(IState state)
		{
			in_Renamed.UndeleteAll(state);
		}
		
		public override bool HasNorms(System.String field, IState state)
		{
			EnsureOpen();
			return in_Renamed.HasNorms(field, state);
		}
		
		public override byte[] Norms(System.String f, IState state)
		{
			EnsureOpen();
			return in_Renamed.Norms(f, state);
		}
		
		public override void  Norms(System.String f, byte[] bytes, int offset, IState state)
		{
			EnsureOpen();
			in_Renamed.Norms(f, bytes, offset, state);
		}
		
		protected internal override void  DoSetNorm(int d, System.String f, byte b, IState state)
		{
			in_Renamed.SetNorm(d, f, b, state);
		}
		
		public override TermEnum Terms(IState state)
		{
			EnsureOpen();
			return in_Renamed.Terms(state);
		}
		
		public override TermEnum Terms(Term t, IState state)
		{
			EnsureOpen();
			return in_Renamed.Terms(t, state);
		}
		
		public override int DocFreq(Term t, IState state)
		{
			EnsureOpen();
			return in_Renamed.DocFreq(t, state);
		}
		
		public override TermDocs TermDocs(IState state)
		{
			EnsureOpen();
			return in_Renamed.TermDocs(state);
		}
		
		public override TermDocs TermDocs(Term term, IState state)
		{
			EnsureOpen();
			return in_Renamed.TermDocs(term, state);
		}
		
		public override TermPositions TermPositions(IState state)
		{
			EnsureOpen();
			return in_Renamed.TermPositions(state);
		}
		
		protected internal override void  DoDelete(int n, IState state)
		{
			in_Renamed.DeleteDocument(n, state);
		}

        protected internal override void DoCommit(System.Collections.Generic.IDictionary<string, string> commitUserData, IState state)
		{
			in_Renamed.Commit(commitUserData, state);
		}
		
		protected internal override void  DoClose(IState state)
		{
			in_Renamed.Close();
            // NOTE: only needed in case someone had asked for
            // FieldCache for top-level reader (which is generally
            // not a good idea):
            Lucene.Net.Search.FieldCache_Fields.DEFAULT.Purge(this);
		}


        public override System.Collections.Generic.ICollection<string> GetFieldNames(IndexReader.FieldOption fieldNames)
		{
			EnsureOpen();
			return in_Renamed.GetFieldNames(fieldNames);
		}

	    public override long Version
	    {
	        get
	        {
	            EnsureOpen();
	            return in_Renamed.Version;
	        }
	    }

	    public override bool IsCurrent(IState state)
	    {
	        EnsureOpen();
	        return in_Renamed.IsCurrent(state);
	    }

	    public override bool IsOptimized()
	    {
	        EnsureOpen();
	        return in_Renamed.IsOptimized();
	    }

	    public override IndexReader[] GetSequentialSubReaders()
	    {
	        return in_Renamed.GetSequentialSubReaders();
	    }

	    override public System.Object Clone(IState state)
		{
            System.Diagnostics.Debug.Fail("Port issue:", "Lets see if we need this FilterIndexReader.Clone()"); // {{Aroush-2.9}}
			return null;
		}

	    /// <summary>
	    /// If the subclass of FilteredIndexReader modifies the
	    /// contents of the FieldCache, you must override this
	    /// method to provide a different key */
	    ///</summary>
	    public override object FieldCacheKey
	    {
	        get { return in_Renamed.FieldCacheKey; }
	    }

	    /// <summary>
	    /// If the subclass of FilteredIndexReader modifies the
	    /// deleted docs, you must override this method to provide
	    /// a different key */
	    /// </summary>
	    public override object DeletesCacheKey
	    {
	        get { return in_Renamed.DeletesCacheKey; }
	    }
	}
}