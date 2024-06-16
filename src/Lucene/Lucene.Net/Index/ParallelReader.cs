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
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Store;
using Lucene.Net.Support;
using Lucene.Net.Util;
using Document = Lucene.Net.Documents.Document;
using FieldSelector = Lucene.Net.Documents.FieldSelector;
using FieldSelectorResult = Lucene.Net.Documents.FieldSelectorResult;

namespace Lucene.Net.Index
{
	/// <summary>An IndexReader which reads multiple, parallel indexes.  Each index added
	/// must have the same number of documents, but typically each contains
	/// different fields.  Each document contains the union of the fields of all
	/// documents with the same document number.  When searching, matches for a
	/// query term are from the first index added that has the field.
	/// 
	/// <p/>This is useful, e.g., with collections that have large fields which
	/// change rarely and small fields that change more frequently.  The smaller
	/// fields may be re-indexed in a new index and both indexes may be searched
	/// together.
	/// 
	/// <p/><strong>Warning:</strong> It is up to you to make sure all indexes
	/// are created and modified the same way. For example, if you add
	/// documents to one index, you need to add the same documents in the
	/// same order to the other indexes. <em>Failure to do so will result in
	/// undefined behavior</em>.
	/// </summary>
	public class ParallelReader:IndexReader, ILuceneCloneable
	{
        private List<IndexReader> readers = new List<IndexReader>();
        private List<bool> decrefOnClose = new List<bool>(); // remember which subreaders to decRef on close
		internal bool incRefReaders = false;
		private SortedDictionary<string, IndexReader> fieldToReader = new SortedDictionary<string, IndexReader>();
		private HashMap<IndexReader, ICollection<string>> readerToFields = new HashMap<IndexReader, ICollection<string>>();
        private List<IndexReader> storedFieldReaders = new List<IndexReader>();
		
		private int maxDoc;
		private int numDocs;
		private bool hasDeletions;
		
		/// <summary>Construct a ParallelReader. 
		/// <p/>Note that all subreaders are closed if this ParallelReader is closed.<p/>
		/// </summary>
		public ParallelReader():this(true)
		{
		}
		
		/// <summary>Construct a ParallelReader. </summary>
		/// <param name="closeSubReaders">indicates whether the subreaders should be closed
		/// when this ParallelReader is closed
		/// </param>
		public ParallelReader(bool closeSubReaders):base()
		{
			this.incRefReaders = !closeSubReaders;
		}
		
		/// <summary>Add an IndexReader.</summary>
		/// <throws>  IOException if there is a low-level IO error </throws>
		public virtual void  Add(IndexReader reader)
		{
			EnsureOpen();
			Add(reader, false);
		}
		
		/// <summary>Add an IndexReader whose stored fields will not be returned.  This can
		/// accellerate search when stored fields are only needed from a subset of
		/// the IndexReaders.
		/// 
		/// </summary>
		/// <throws>  IllegalArgumentException if not all indexes contain the same number </throws>
		/// <summary>     of documents
		/// </summary>
		/// <throws>  IllegalArgumentException if not all indexes have the same value </throws>
		/// <summary>     of <see cref="IndexReader.MaxDoc" />
		/// </summary>
		/// <throws>  IOException if there is a low-level IO error </throws>
		public virtual void  Add(IndexReader reader, bool ignoreStoredFields)
		{
			
			EnsureOpen();
			if (readers.Count == 0)
			{
				this.maxDoc = reader.MaxDoc;
				this.numDocs = reader.NumDocs();
				this.hasDeletions = reader.HasDeletions;
			}
			
			if (reader.MaxDoc != maxDoc)
			// check compatibility
				throw new System.ArgumentException("All readers must have same maxDoc: " + maxDoc + "!=" + reader.MaxDoc);
			if (reader.NumDocs() != numDocs)
				throw new System.ArgumentException("All readers must have same numDocs: " + numDocs + "!=" + reader.NumDocs());
			
			ICollection<string> fields = reader.GetFieldNames(IndexReader.FieldOption.ALL);
			readerToFields[reader] = fields;
			foreach(var field in fields)
			{
				// update fieldToReader map
                // Do a containskey firt to mimic java behavior
				if (!fieldToReader.ContainsKey(field) || fieldToReader[field] == null)
					fieldToReader[field] = reader;
			}
			
			if (!ignoreStoredFields)
				storedFieldReaders.Add(reader); // add to storedFieldReaders
			readers.Add(reader);
			
			if (incRefReaders)
			{
				reader.IncRef();
			}
			decrefOnClose.Add(incRefReaders);
		}
		
		public override System.Object Clone(IState state)
		{
			try
			{
				return DoReopen(true, state);
			}
			catch (System.Exception ex)
			{
				throw new System.SystemException(ex.Message, ex);
			}
		}
		
		/// <summary> Tries to reopen the subreaders.
		/// <br/>
		/// If one or more subreaders could be re-opened (i. e. subReader.reopen() 
		/// returned a new instance != subReader), then a new ParallelReader instance 
		/// is returned, otherwise this instance is returned.
		/// <p/>
		/// A re-opened instance might share one or more subreaders with the old 
		/// instance. Index modification operations result in undefined behavior
		/// when performed before the old instance is closed.
		/// (see <see cref="IndexReader.Reopen()" />).
		/// <p/>
		/// If subreaders are shared, then the reference count of those
		/// readers is increased to ensure that the subreaders remain open
		/// until the last referring reader is closed.
		/// 
		/// </summary>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error  </throws>
		public override IndexReader Reopen(IState state)
		{
			lock (this)
			{
				return DoReopen(false, state);
			}
		}
		
		protected internal virtual IndexReader DoReopen(bool doClone, IState state)
		{
			EnsureOpen();
			
			bool reopened = false;
            IList<IndexReader> newReaders = new List<IndexReader>();
			
			bool success = false;
			
			try
			{
				foreach(var oldReader in readers)
				{
					IndexReader newReader = null;
					if (doClone)
					{
						newReader = (IndexReader) oldReader.Clone(state);
					}
					else
					{
						newReader = oldReader.Reopen(state);
					}
					newReaders.Add(newReader);
					// if at least one of the subreaders was updated we remember that
					// and return a new ParallelReader
					if (newReader != oldReader)
					{
						reopened = true;
					}
				}
				success = true;
			}
			finally
			{
				if (!success && reopened)
				{
					for (int i = 0; i < newReaders.Count; i++)
					{
						IndexReader r = newReaders[i];
						if (r != readers[i])
						{
							try
							{
								r.Close();
							}
							catch (System.IO.IOException)
							{
								// keep going - we want to clean up as much as possible
							}
						}
					}
				}
			}
			
			if (reopened)
			{
                List<bool> newDecrefOnClose = new List<bool>();
				ParallelReader pr = new ParallelReader();
				for (int i = 0; i < readers.Count; i++)
				{
					IndexReader oldReader = readers[i];
					IndexReader newReader = newReaders[i];
					if (newReader == oldReader)
					{
						newDecrefOnClose.Add(true);
						newReader.IncRef();
					}
					else
					{
						// this is a new subreader instance, so on close() we don't
						// decRef but close it 
						newDecrefOnClose.Add(false);
					}
					pr.Add(newReader, !storedFieldReaders.Contains(oldReader));
				}
				pr.decrefOnClose = newDecrefOnClose;
				pr.incRefReaders = incRefReaders;
				return pr;
			}
			else
			{
				// No subreader was refreshed
				return this;
			}
		}


	    public override int NumDocs()
	    {
	        // Don't call ensureOpen() here (it could affect performance)
	        return numDocs;
	    }

	    public override int MaxDoc
	    {
	        get
	        {
	            // Don't call ensureOpen() here (it could affect performance)
	            return maxDoc;
	        }
	    }

	    public override bool HasDeletions
	    {
	        get
	        {
	            // Don't call ensureOpen() here (it could affect performance)
	            return hasDeletions;
	        }
	    }

	    // check first reader
		public override bool IsDeleted(int n)
		{
			// Don't call ensureOpen() here (it could affect performance)
			if (readers.Count > 0)
				return readers[0].IsDeleted(n);
			return false;
		}
		
		// delete in all readers
		protected internal override void  DoDelete(int n, IState state)
		{
			foreach(var reader in readers)
			{
				reader.DeleteDocument(n, state);
			}
			hasDeletions = true;
		}
		
		// undeleteAll in all readers
		protected internal override void  DoUndeleteAll(IState state)
		{
			foreach(var reader in readers)
			{
				reader.UndeleteAll(state);
			}
			hasDeletions = false;
		}
		
		// append fields from storedFieldReaders
		public override Document Document(int n, FieldSelector fieldSelector, IState state)
		{
			EnsureOpen();
			Document result = new Document();
			foreach(IndexReader reader in storedFieldReaders)
			{
				bool include = (fieldSelector == null);
				if (!include)
				{
				    var fields = readerToFields[reader];
					foreach(var field in fields)
					{
                        if (fieldSelector.Accept(field) != FieldSelectorResult.NO_LOAD)
						{
							include = true;
							break;
						}
					}
				}
				if (include)
				{
				    var fields = reader.Document(n, fieldSelector, state).GetFields();
					foreach(var field in fields)
					{
                        result.Add(field);
					}
				}
			}
			return result;
		}
		
		// get all vectors
		public override ITermFreqVector[] GetTermFreqVectors(int n, IState state)
		{
			EnsureOpen();
			IList<ITermFreqVector> results = new List<ITermFreqVector>();
            foreach(var e in fieldToReader)
			{
				System.String field = e.Key;
				IndexReader reader = e.Value;

				ITermFreqVector vector = reader.GetTermFreqVector(n, field, state);
				if (vector != null)
					results.Add(vector);
			}
			return results.ToArray();
		}
		
		public override ITermFreqVector GetTermFreqVector(int n, System.String field, IState state)
		{
			EnsureOpen();
			IndexReader reader = (fieldToReader[field]);
			return reader == null?null:reader.GetTermFreqVector(n, field, state);
		}
		
		
		public override void  GetTermFreqVector(int docNumber, System.String field, TermVectorMapper mapper, IState state)
		{
			EnsureOpen();
			IndexReader reader = (fieldToReader[field]);
			if (reader != null)
			{
				reader.GetTermFreqVector(docNumber, field, mapper, state);
			}
		}
		
		public override void  GetTermFreqVector(int docNumber, TermVectorMapper mapper, IState state)
		{
			EnsureOpen();

            foreach(var e in fieldToReader)
			{
				System.String field = e.Key;
				IndexReader reader = e.Value;
				reader.GetTermFreqVector(docNumber, field, mapper, state);
			}
		}
		
		public override bool HasNorms(System.String field, IState state)
		{
			EnsureOpen();
			IndexReader reader = fieldToReader[field];
		    return reader != null && reader.HasNorms(field, state);
		}
		
		public override byte[] Norms(System.String field, IState state)
		{
			EnsureOpen();
			IndexReader reader = fieldToReader[field];
			return reader == null?null:reader.Norms(field, state);
		}
		
		public override void  Norms(System.String field, byte[] result, int offset, IState state)
		{
			EnsureOpen();
			IndexReader reader = fieldToReader[field];
			if (reader != null)
				reader.Norms(field, result, offset, state);
		}
		
		protected internal override void  DoSetNorm(int n, System.String field, byte value_Renamed, IState state)
		{
			IndexReader reader = fieldToReader[field];
			if (reader != null)
				reader.DoSetNorm(n, field, value_Renamed, state);
		}
		
		public override TermEnum Terms(IState state)
		{
			EnsureOpen();
			return new ParallelTermEnum(this, state);
		}
		
		public override TermEnum Terms(Term term, IState state)
		{
			EnsureOpen();
			return new ParallelTermEnum(this, term, state);
		}
		
		public override int DocFreq(Term term, IState state)
		{
			EnsureOpen();
			IndexReader reader = fieldToReader[term.Field];
			return reader == null?0:reader.DocFreq(term, state);
		}
		
		public override TermDocs TermDocs(Term term, IState state)
		{
			EnsureOpen();
			return new ParallelTermDocs(this, term, state);
		}
		
		public override TermDocs TermDocs(IState state)
		{
			EnsureOpen();
			return new ParallelTermDocs(this);
		}
		
		public override TermPositions TermPositions(Term term, IState state)
		{
			EnsureOpen();
			return new ParallelTermPositions(this, term, state);
		}
		
		public override TermPositions TermPositions(IState state)
		{
			EnsureOpen();
			return new ParallelTermPositions(this);
		}

	    /// <summary> Checks recursively if all subreaders are up to date. </summary>
	    public override bool IsCurrent(IState state)
	    {
	        foreach (var reader in readers)
	        {
	            if (!reader.IsCurrent(state))
	            {
	                return false;
	            }
	        }

	        // all subreaders are up to date
	        return true;
	    }

	    /// <summary> Checks recursively if all subindexes are optimized </summary>
	    public override bool IsOptimized()
	    {
	        foreach (var reader in readers)
	        {
	            if (!reader.IsOptimized())
	            {
	                return false;
	            }
	        }

	        // all subindexes are optimized
	        return true;
	    }


	    /// <summary>Not implemented.</summary>
	    /// <throws>  UnsupportedOperationException </throws>
	    public override long Version
	    {
	        get { throw new System.NotSupportedException("ParallelReader does not support this method."); }
	    }

	    // for testing
		public /*internal*/ virtual IndexReader[] GetSubReaders()
		{
			return readers.ToArray();
		}

        protected internal override void DoCommit(IDictionary<string, string> commitUserData, IState state)
		{
			foreach(var reader in readers)
				reader.Commit(commitUserData, state);
		}
		
		protected internal override void  DoClose(IState state)
		{
			lock (this)
			{
				for (int i = 0; i < readers.Count; i++)
				{
					if (decrefOnClose[i])
					{
						readers[i].DecRef(state);
					}
					else
					{
						readers[i].Close();
					}
				}
			}

            Lucene.Net.Search.FieldCache_Fields.DEFAULT.Purge(this);
		}

        public override System.Collections.Generic.ICollection<string> GetFieldNames(IndexReader.FieldOption fieldNames)
		{
			EnsureOpen();
            ISet<string> fieldSet = Lucene.Net.Support.Compatibility.SetFactory.CreateHashSet<string>();
			foreach(var reader in readers)
			{
				ICollection<string> names = reader.GetFieldNames(fieldNames);
                fieldSet.UnionWith(names);
			}
			return fieldSet;
		}
		
		private class ParallelTermEnum : TermEnum
		{
			private void  InitBlock(ParallelReader enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private ParallelReader enclosingInstance;
			public ParallelReader Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private System.String field;
			private IEnumerator<string> fieldIterator;
			private TermEnum termEnum;

		    private bool isDisposed;
			
			public ParallelTermEnum(ParallelReader enclosingInstance, IState state)
			{
				InitBlock(enclosingInstance);
				try
				{
					field = Enclosing_Instance.fieldToReader.Keys.First();
				}
				catch (ArgumentOutOfRangeException)
				{
					// No fields, so keep field == null, termEnum == null
					return;
				}
				if (field != null)
					termEnum = Enclosing_Instance.fieldToReader[field].Terms(state);
			}
			
			public ParallelTermEnum(ParallelReader enclosingInstance, Term term, IState state)
			{
				InitBlock(enclosingInstance);
				field = term.Field;
				IndexReader reader = Enclosing_Instance.fieldToReader[field];
				if (reader != null)
					termEnum = reader.Terms(term, state);
			}
			
			public override bool Next(IState state)
			{
				if (termEnum == null)
					return false;
				
				// another term in this field?
				if (termEnum.Next(state) && (System.Object) termEnum.Term.Field == (System.Object) field)
					return true; // yes, keep going
				
				termEnum.Close(); // close old termEnum
				
				// find the next field with terms, if any
				if (fieldIterator == null)
				{
                    var newList = new List<string>();  
                    if (Enclosing_Instance.fieldToReader != null && Enclosing_Instance.fieldToReader.Count > 0)
                    {
                        var comparer = Enclosing_Instance.fieldToReader.Comparer;
                        foreach(var entry in Enclosing_Instance.fieldToReader.Keys.Where(x => comparer.Compare(x, field) >= 0))
                            newList.Add(entry);
                    }

                    fieldIterator = newList.Skip(1).GetEnumerator(); // Skip field to get next one
				}
				while (fieldIterator.MoveNext())
				{
					field = fieldIterator.Current;
					termEnum = Enclosing_Instance.fieldToReader[field].Terms(new Term(field), state);
					Term term = termEnum.Term;
					if (term != null && (System.Object) term.Field == (System.Object) field)
						return true;
					else
						termEnum.Close();
				}
				
				return false; // no more fields
			}

		    public override Term Term
		    {
		        get
		        {
		            if (termEnum == null)
		                return null;

		            return termEnum.Term;
		        }
		    }

		    public override int DocFreq()
			{
				if (termEnum == null)
					return 0;
				
				return termEnum.DocFreq();
			}

            protected override void Dispose(bool disposing)
            {
                if (isDisposed) return;

                if (disposing)
                {
                    if (termEnum != null)
                        termEnum.Close();
                }

                isDisposed = true;
            }
		}
		
		// wrap a TermDocs in order to support seek(Term)
		private class ParallelTermDocs : TermDocs
		{
			private void  InitBlock(ParallelReader enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private ParallelReader enclosingInstance;
			public ParallelReader Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			protected internal TermDocs termDocs;

		    private bool isDisposed;
			
			public ParallelTermDocs(ParallelReader enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			public ParallelTermDocs(ParallelReader enclosingInstance, Term term, IState state)
			{
				InitBlock(enclosingInstance);
                if(term == null)
                    termDocs = (Enclosing_Instance.readers.Count == 0)
                                   ? null
                                   : Enclosing_Instance.readers[0].TermDocs(null, state);
                else
                    Seek(term, state);
			}

		    public virtual int Doc
		    {
		        get { return termDocs.Doc; }
		    }

		    public virtual int Freq
		    {
		        get { return termDocs.Freq; }
		    }

		    public virtual void  Seek(Term term, IState state)
			{
				IndexReader reader = Enclosing_Instance.fieldToReader[term.Field];
				termDocs = reader != null?reader.TermDocs(term, state):null;
			}
			
			public virtual void  Seek(TermEnum termEnum, IState state)
			{
				Seek(termEnum.Term, state);
			}
			
			public virtual bool Next(IState state)
			{
				if (termDocs == null)
					return false;
				
				return termDocs.Next(state);
			}
			
			public virtual int Read(Span<int> docs, Span<int> freqs, IState state)
			{
				if (termDocs == null)
					return 0;
				
				return termDocs.Read(docs, freqs, state);
			}
			
			public virtual bool SkipTo(int target, IState state)
			{
				if (termDocs == null)
					return false;
				
				return termDocs.SkipTo(target, state);
			}

            [Obsolete("Use Dispose() instead")]
            public virtual void Close()
            {
                Dispose();
            }

		    public void Dispose()
		    {
		        Dispose(true);
		    }

            protected virtual void Dispose(bool disposing)
            {
                if (isDisposed) return;

                if (disposing)
                {
                    if (termDocs != null)
                        termDocs.Close();
                }

                isDisposed = true;
            }
		}
		
		private class ParallelTermPositions:ParallelTermDocs, TermPositions
		{
			private void  InitBlock(ParallelReader enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private ParallelReader enclosingInstance;
			public new ParallelReader Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			public ParallelTermPositions(ParallelReader enclosingInstance):base(enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			public ParallelTermPositions(ParallelReader enclosingInstance, Term term, IState state) :base(enclosingInstance)
			{
				InitBlock(enclosingInstance);
				Seek(term, state);
			}
			
			public override void  Seek(Term term, IState state)
			{
				IndexReader reader = Enclosing_Instance.fieldToReader[term.Field];
				termDocs = reader != null?reader.TermPositions(term, state):null;
			}
			
			public virtual int NextPosition(IState state)
			{
				// It is an error to call this if there is no next position, e.g. if termDocs==null
				return ((TermPositions) termDocs).NextPosition(state);
			}

		    public virtual int PayloadLength
		    {
		        get { return ((TermPositions) termDocs).PayloadLength; }
		    }

		    public virtual byte[] GetPayload(byte[] data, int offset, IState state)
			{
				return ((TermPositions) termDocs).GetPayload(data, offset, state);
			}
			
			
			// TODO: Remove warning after API has been finalized

		    public virtual bool IsPayloadAvailable
		    {
		        get { return ((TermPositions) termDocs).IsPayloadAvailable; }
		    }
		}
	}
}