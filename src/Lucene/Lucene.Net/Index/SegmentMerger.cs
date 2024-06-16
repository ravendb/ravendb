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
using Lucene.Net.Store;
using Document = Lucene.Net.Documents.Document;
using FieldSelector = Lucene.Net.Documents.FieldSelector;
using FieldSelectorResult = Lucene.Net.Documents.FieldSelectorResult;
using FieldOption = Lucene.Net.Index.IndexReader.FieldOption;
using MergeAbortedException = Lucene.Net.Index.MergePolicy.MergeAbortedException;
using Directory = Lucene.Net.Store.Directory;
using IndexInput = Lucene.Net.Store.IndexInput;
using IndexOutput = Lucene.Net.Store.IndexOutput;

namespace Lucene.Net.Index
{
	
	/// <summary> The SegmentMerger class combines two or more Segments, represented by an IndexReader (<see cref="Add" />,
	/// into a single Segment.  After adding the appropriate readers, call the merge method to combine the 
	/// segments.
	/// <p/> 
	/// If the compoundFile flag is set, then the segments will be merged into a compound file.
	/// 
	/// 
	/// </summary>
	/// <seealso cref="Merge()">
	/// </seealso>
	/// <seealso cref="Add">
	/// </seealso>
	public sealed class SegmentMerger
	{
		private class OptimizeScopeCheckAbort : CheckAbort
		{
			private readonly OptimizeScope _scope;

			public OptimizeScopeCheckAbort(MergePolicy.OneMerge merge, Directory dir, OptimizeScope scope) : base(merge, dir)
			{
				_scope = scope;
			}

			/// <summary> Records the fact that roughly units amount of work
			/// have been done since this method was last called.
			/// When adding time-consuming code into SegmentMerger,
			/// you should test different values for units to ensure
			/// that the time in between calls to merge.checkAborted
			/// is up to ~ 1 second.
			/// </summary>
			public override void Work(double units, IState state)
			{
				workCount += units;
				if (workCount >= 10000.0)
				{
					if (_scope.Token.IsCancellationRequested)
					{
						merge.Abort();
						_scope.Token.ThrowIfCancellationRequested();
					}
					merge.CheckAborted(dir, state);
					workCount = 0;
				}
			}
		}
		
		private class AnonymousClassCheckAbort:CheckAbort
		{
			private void  InitBlock(SegmentMerger enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private SegmentMerger enclosingInstance;
			public SegmentMerger Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal AnonymousClassCheckAbort(SegmentMerger enclosingInstance, Lucene.Net.Index.MergePolicy.OneMerge Param1, Lucene.Net.Store.Directory Param2):base(Param1, Param2)
			{
				InitBlock(enclosingInstance);
			}
			public override void  Work(double units, IState state)
			{
				// do nothing
			}
		}
		private class AnonymousClassCheckAbort1:CheckAbort
		{
			private void  InitBlock(SegmentMerger enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private SegmentMerger enclosingInstance;
			public SegmentMerger Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal AnonymousClassCheckAbort1(SegmentMerger enclosingInstance, Lucene.Net.Index.MergePolicy.OneMerge Param1, Lucene.Net.Store.Directory Param2):base(Param1, Param2)
			{
				InitBlock(enclosingInstance);
			}
			public override void  Work(double units, IState state)
			{
				// do nothing
			}
		}

		private void  InitBlock()
		{
			termIndexInterval = IndexWriter.DEFAULT_TERM_INDEX_INTERVAL;
		}
		
		/// <summary>norms header placeholder </summary>
		internal static readonly byte[] NORMS_HEADER = new byte[]{(byte) 'N', (byte) 'R', (byte) 'M', unchecked((byte) - 1)};
		
		private Directory directory;
		private System.String segment;
		private int termIndexInterval;
		
		private IList<IndexReader> readers = new List<IndexReader>();
		private FieldInfos fieldInfos;
		
		private int mergedDocs;
		
		private CheckAbort checkAbort;
		
		// Whether we should merge doc stores (stored fields and
		// vectors files).  When all segments we are merging
		// already share the same doc store files, we don't need
		// to merge the doc stores.
		private bool mergeDocStores;
		
		/// <summary>Maximum number of contiguous documents to bulk-copy
		/// when merging stored fields 
		/// </summary>
		private const int MAX_RAW_MERGE_DOCS = 4192;
		
		/// <summary>This ctor used only by test code.
		/// 
		/// </summary>
		/// <param name="dir">The Directory to merge the other segments into
		/// </param>
		/// <param name="name">The name of the new segment
		/// </param>
		public /*internal*/ SegmentMerger(Directory dir, System.String name)
		{
			InitBlock();
			directory = dir;
			segment = name;
			checkAbort = new AnonymousClassCheckAbort(this, null, null);
		}
		
		internal SegmentMerger(IndexWriter writer, System.String name, MergePolicy.OneMerge merge)
		{
			InitBlock();
			directory = writer.Directory;
			segment = name;
			if (merge != null)
			{
				checkAbort = merge.optimize == false 
					? new CheckAbort(merge, directory) 
					: new OptimizeScopeCheckAbort(merge, directory, writer.OptimizeScope);
			}
			else
			{
				checkAbort = new AnonymousClassCheckAbort1(this, null, null);
			}
			termIndexInterval = writer.TermIndexInterval;
		}
		
		internal bool HasProx()
		{
			return fieldInfos.HasProx();
		}
		
		/// <summary> Add an IndexReader to the collection of readers that are to be merged</summary>
		/// <param name="reader">
		/// </param>
		public /*internal*/ void  Add(IndexReader reader)
		{
			readers.Add(reader);
		}
		
		/// <summary> </summary>
		/// <param name="i">The index of the reader to return
		/// </param>
		/// <returns> The ith reader to be merged
		/// </returns>
		internal IndexReader SegmentReader(int i)
		{
			return readers[i];
		}
		
		/// <summary> Merges the readers specified by the <see cref="Add" /> method into the directory passed to the constructor</summary>
		/// <returns> The number of documents that were merged
		/// </returns>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		public /*internal*/ int Merge(IState state)
		{
			return Merge(true, state);
		}
		
		/// <summary> Merges the readers specified by the <see cref="Add" /> method
		/// into the directory passed to the constructor.
		/// </summary>
		/// <param name="mergeDocStores">if false, we will not merge the
		/// stored fields nor vectors files
		/// </param>
		/// <returns> The number of documents that were merged
		/// </returns>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		internal int Merge(bool mergeDocStores, IState state)
		{
			
			this.mergeDocStores = mergeDocStores;
			
			// NOTE: it's important to add calls to
			// checkAbort.work(...) if you make any changes to this
			// method that will spend alot of time.  The frequency
			// of this check impacts how long
			// IndexWriter.close(false) takes to actually stop the
			// threads.
			
			mergedDocs = MergeFields(state);
			MergeTerms(state);
			MergeNorms(state);
			
			if (mergeDocStores && fieldInfos.HasVectors())
				MergeVectors(state);
			
			return mergedDocs;
		}
		
		/// <summary> close all IndexReaders that have been added.
		/// Should not be called before merge().
		/// </summary>
		/// <throws>  IOException </throws>
		internal void  CloseReaders()
		{
			foreach(IndexReader reader in readers)
			{
				reader.Dispose();
			}
		}

        internal ICollection<string> GetMergedFiles()
		{
            ISet<string> fileSet = Lucene.Net.Support.Compatibility.SetFactory.CreateHashSet<string>();
			
			// Basic files
			for (int i = 0; i < IndexFileNames.COMPOUND_EXTENSIONS.Length; i++)
			{
				System.String ext = IndexFileNames.COMPOUND_EXTENSIONS[i];
				
				if (ext.Equals(IndexFileNames.PROX_EXTENSION) && !HasProx())
					continue;
				
				if (mergeDocStores || (!ext.Equals(IndexFileNames.FIELDS_EXTENSION) && !ext.Equals(IndexFileNames.FIELDS_INDEX_EXTENSION)))
                    fileSet.Add(segment + "." + ext);
			}
			
			// Fieldable norm files
			for (int i = 0; i < fieldInfos.Size(); i++)
			{
				FieldInfo fi = fieldInfos.FieldInfo(i);
				if (fi.isIndexed && !fi.omitNorms)
				{
                    fileSet.Add(segment + "." + IndexFileNames.NORMS_EXTENSION);
					break;
				}
			}
			
			// Vector files
			if (fieldInfos.HasVectors() && mergeDocStores)
			{
				for (int i = 0; i < IndexFileNames.VECTOR_EXTENSIONS.Length; i++)
				{
                    fileSet.Add(segment + "." + IndexFileNames.VECTOR_EXTENSIONS[i]);
				}
			}

            return fileSet;
        }

        public /*internal*/ ICollection<string> CreateCompoundFile(System.String fileName)
        {
            ICollection<string> files = GetMergedFiles();
            CompoundFileWriter cfsWriter = new CompoundFileWriter(directory, fileName, checkAbort);

			// Now merge all added files
			foreach(var file in files)
			{
				cfsWriter.AddFile(file);
			}
			
			// Perform the merge
			cfsWriter.Close();

            return files;
		}

        private void AddIndexed(IndexReader reader, FieldInfos fInfos, ICollection<string> names, bool storeTermVectors, bool storePositionWithTermVector, bool storeOffsetWithTermVector, bool storePayloads, bool omitTFAndPositions, IState state)
        {
            foreach (var field in names)
            {
                fInfos.Add(field, true, storeTermVectors, storePositionWithTermVector, storeOffsetWithTermVector,
                           !reader.HasNorms(field, state), storePayloads, omitTFAndPositions);
            }
        }

	    private SegmentReader[] matchingSegmentReaders;
		private int[] rawDocLengths;
		private int[] rawDocLengths2;
		
		private void  SetMatchingSegmentReaders()
		{
			// If the i'th reader is a SegmentReader and has
			// identical fieldName -> number mapping, then this
			// array will be non-null at position i:
			int numReaders = readers.Count;
			matchingSegmentReaders = new SegmentReader[numReaders];
			
			// If this reader is a SegmentReader, and all of its
			// field name -> number mappings match the "merged"
			// FieldInfos, then we can do a bulk copy of the
			// stored fields:
			for (int i = 0; i < numReaders; i++)
			{
				IndexReader reader = readers[i];
				if (reader is SegmentReader)
				{
					SegmentReader segmentReader = (SegmentReader) reader;
					bool same = true;
					FieldInfos segmentFieldInfos = segmentReader.FieldInfos();
					int numFieldInfos = segmentFieldInfos.Size();
					for (int j = 0; same && j < numFieldInfos; j++)
					{
						same = fieldInfos.FieldName(j).Equals(segmentFieldInfos.FieldName(j));
					}
					if (same)
					{
						matchingSegmentReaders[i] = segmentReader;
					}
				}
			}
			
			// Used for bulk-reading raw bytes for stored fields
			rawDocLengths = new int[MAX_RAW_MERGE_DOCS];
			rawDocLengths2 = new int[MAX_RAW_MERGE_DOCS];
		}
		
		/// <summary> </summary>
		/// <returns> The number of documents in all of the readers
		/// </returns>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		private int MergeFields(IState state)
		{
			
			if (!mergeDocStores)
			{
				// When we are not merging by doc stores, their field
				// name -> number mapping are the same.  So, we start
				// with the fieldInfos of the last segment in this
				// case, to keep that numbering.
				SegmentReader sr = (SegmentReader) readers[readers.Count - 1];
				fieldInfos = (FieldInfos) sr.core.fieldInfos.Clone();
			}
			else
			{
				fieldInfos = new FieldInfos(); // merge field names
			}
			
			foreach(IndexReader reader in readers)
			{
				if (reader is SegmentReader)
				{
					SegmentReader segmentReader = (SegmentReader) reader;
					FieldInfos readerFieldInfos = segmentReader.FieldInfos();
					int numReaderFieldInfos = readerFieldInfos.Size();
					for (int j = 0; j < numReaderFieldInfos; j++)
					{
						FieldInfo fi = readerFieldInfos.FieldInfo(j);
						fieldInfos.Add(fi.name, fi.isIndexed, fi.storeTermVector, fi.storePositionWithTermVector, fi.storeOffsetWithTermVector, !reader.HasNorms(fi.name, state), fi.storePayloads, fi.omitTermFreqAndPositions);
					}
				}
				else
				{
					AddIndexed(reader, fieldInfos, reader.GetFieldNames(FieldOption.TERMVECTOR_WITH_POSITION_OFFSET), true, true, true, false, false, state);
					AddIndexed(reader, fieldInfos, reader.GetFieldNames(FieldOption.TERMVECTOR_WITH_POSITION), true, true, false, false, false, state);
					AddIndexed(reader, fieldInfos, reader.GetFieldNames(FieldOption.TERMVECTOR_WITH_OFFSET), true, false, true, false, false, state);
					AddIndexed(reader, fieldInfos, reader.GetFieldNames(FieldOption.TERMVECTOR), true, false, false, false, false, state);
					AddIndexed(reader, fieldInfos, reader.GetFieldNames(FieldOption.OMIT_TERM_FREQ_AND_POSITIONS), false, false, false, false, true, state);
					AddIndexed(reader, fieldInfos, reader.GetFieldNames(FieldOption.STORES_PAYLOADS), false, false, false, true, false, state);
					AddIndexed(reader, fieldInfos, reader.GetFieldNames(FieldOption.INDEXED), false, false, false, false, false, state);
					fieldInfos.Add(reader.GetFieldNames(FieldOption.UNINDEXED), false);
				}
			}
			fieldInfos.Write(directory, segment + ".fnm", state);
			
			int docCount = 0;
			
			SetMatchingSegmentReaders();
			
			if (mergeDocStores)
			{
				// merge field values
				FieldsWriter fieldsWriter = new FieldsWriter(directory, segment, fieldInfos, state);
				
				try
				{
					int idx = 0;
					foreach(IndexReader reader in readers)
					{
						SegmentReader matchingSegmentReader = matchingSegmentReaders[idx++];
						FieldsReader matchingFieldsReader = null;
						if (matchingSegmentReader != null)
						{
							FieldsReader fieldsReader = matchingSegmentReader.GetFieldsReader(state);
							if (fieldsReader != null && fieldsReader.CanReadRawDocs())
							{
								matchingFieldsReader = fieldsReader;
							}
						}
						if (reader.HasDeletions)
						{
							docCount += CopyFieldsWithDeletions(fieldsWriter, reader, matchingFieldsReader, state);
						}
						else
						{
							docCount += CopyFieldsNoDeletions(fieldsWriter, reader, matchingFieldsReader, state);
						}
					}
				}
				finally
				{
					fieldsWriter.Dispose();
				}
				
				System.String fileName = segment + "." + IndexFileNames.FIELDS_INDEX_EXTENSION;
				long fdxFileLength = directory.FileLength(fileName, state);
				
				if (4 + ((long) docCount) * 8 != fdxFileLength)
				// This is most likely a bug in Sun JRE 1.6.0_04/_05;
				// we detect that the bug has struck, here, and
				// throw an exception to prevent the corruption from
				// entering the index.  See LUCENE-1282 for
				// details.
					throw new System.SystemException("mergeFields produced an invalid result: docCount is " + docCount + " but fdx file size is " + fdxFileLength + " file=" + fileName + " file exists?=" + directory.FileExists(fileName, state) + "; now aborting this merge to prevent index corruption");
			}
			// If we are skipping the doc stores, that means there
			// are no deletions in any of these segments, so we
			// just sum numDocs() of each segment to get total docCount
			else
			{
				foreach(IndexReader reader in readers)
				{
					docCount += reader.NumDocs();
				}
			}
			
			return docCount;
		}
		
		private int CopyFieldsWithDeletions(FieldsWriter fieldsWriter, IndexReader reader, FieldsReader matchingFieldsReader, IState state)
		{
			int docCount = 0;
			int maxDoc = reader.MaxDoc;
			if (matchingFieldsReader != null)
			{
				// We can bulk-copy because the fieldInfos are "congruent"
				for (int j = 0; j < maxDoc; )
				{
					if (reader.IsDeleted(j))
					{
						// skip deleted docs
						++j;
						continue;
					}
					// We can optimize this case (doing a bulk byte copy) since the field 
					// numbers are identical
					int start = j, numDocs = 0;
					do 
					{
						j++;
						numDocs++;
						if (j >= maxDoc)
							break;
						if (reader.IsDeleted(j))
						{
							j++;
							break;
						}
					}
					while (numDocs < MAX_RAW_MERGE_DOCS);
					
					IndexInput stream = matchingFieldsReader.RawDocs(rawDocLengths, start, numDocs, state);
					fieldsWriter.AddRawDocuments(stream, rawDocLengths, numDocs, state);
					docCount += numDocs;
					checkAbort.Work(300 * numDocs, state);
				}
			}
			else
			{
				for (int j = 0; j < maxDoc; j++)
				{
					if (reader.IsDeleted(j))
					{
						// skip deleted docs
						continue;
					}
					// NOTE: it's very important to first assign to doc then pass it to
					// termVectorsWriter.addAllDocVectors; see LUCENE-1282
					Document doc = reader.Document(j, state);
					fieldsWriter.AddDocument(doc, state);
					docCount++;
					checkAbort.Work(300, state);
				}
			}
			return docCount;
		}
		
		private int CopyFieldsNoDeletions(FieldsWriter fieldsWriter, IndexReader reader, FieldsReader matchingFieldsReader, IState state)
		{
			int maxDoc = reader.MaxDoc;
			int docCount = 0;
			if (matchingFieldsReader != null)
			{
				// We can bulk-copy because the fieldInfos are "congruent"
				while (docCount < maxDoc)
				{
					int len = System.Math.Min(MAX_RAW_MERGE_DOCS, maxDoc - docCount);
					IndexInput stream = matchingFieldsReader.RawDocs(rawDocLengths, docCount, len, state);
					fieldsWriter.AddRawDocuments(stream, rawDocLengths, len, state);
					docCount += len;
					checkAbort.Work(300 * len, state);
				}
			}
			else
			{
				for (; docCount < maxDoc; docCount++)
				{
					// NOTE: it's very important to first assign to doc then pass it to
					// termVectorsWriter.addAllDocVectors; see LUCENE-1282
					Document doc = reader.Document(docCount, state);
					fieldsWriter.AddDocument(doc, state);
					checkAbort.Work(300, state);
				}
			}
			return docCount;
		}
		
		/// <summary> Merge the TermVectors from each of the segments into the new one.</summary>
		/// <throws>  IOException </throws>
		private void  MergeVectors(IState state)
		{
			TermVectorsWriter termVectorsWriter = new TermVectorsWriter(directory, segment, fieldInfos, state);
			
			try
			{
				int idx = 0;
				foreach(IndexReader reader in readers)
				{
					SegmentReader matchingSegmentReader = matchingSegmentReaders[idx++];
					TermVectorsReader matchingVectorsReader = null;
					if (matchingSegmentReader != null)
					{
						TermVectorsReader vectorsReader = matchingSegmentReader.GetTermVectorsReaderOrig();
						
						// If the TV* files are an older format then they cannot read raw docs:
						if (vectorsReader != null && vectorsReader.CanReadRawDocs())
						{
							matchingVectorsReader = vectorsReader;
						}
					}
					if (reader.HasDeletions)
					{
						CopyVectorsWithDeletions(termVectorsWriter, matchingVectorsReader, reader, state);
					}
					else
					{
						CopyVectorsNoDeletions(termVectorsWriter, matchingVectorsReader, reader, state);
					}
				}
			}
			finally
			{
				termVectorsWriter.Dispose();
			}
			
			System.String fileName = segment + "." + IndexFileNames.VECTORS_INDEX_EXTENSION;
			long tvxSize = directory.FileLength(fileName, state);
			
			if (4 + ((long) mergedDocs) * 16 != tvxSize)
			// This is most likely a bug in Sun JRE 1.6.0_04/_05;
			// we detect that the bug has struck, here, and
			// throw an exception to prevent the corruption from
			// entering the index.  See LUCENE-1282 for
			// details.
				throw new System.SystemException("mergeVectors produced an invalid result: mergedDocs is " + mergedDocs + " but tvx size is " + tvxSize + " file=" + fileName + " file exists?=" + directory.FileExists(fileName, state) + "; now aborting this merge to prevent index corruption");
		}
		
		private void  CopyVectorsWithDeletions(TermVectorsWriter termVectorsWriter, TermVectorsReader matchingVectorsReader, IndexReader reader, IState state)
		{
			int maxDoc = reader.MaxDoc;
			if (matchingVectorsReader != null)
			{
				// We can bulk-copy because the fieldInfos are "congruent"
				for (int docNum = 0; docNum < maxDoc; )
				{
					if (reader.IsDeleted(docNum))
					{
						// skip deleted docs
						++docNum;
						continue;
					}
					// We can optimize this case (doing a bulk byte copy) since the field 
					// numbers are identical
					int start = docNum, numDocs = 0;
					do 
					{
						docNum++;
						numDocs++;
						if (docNum >= maxDoc)
							break;
						if (reader.IsDeleted(docNum))
						{
							docNum++;
							break;
						}
					}
					while (numDocs < MAX_RAW_MERGE_DOCS);
					
					matchingVectorsReader.RawDocs(rawDocLengths, rawDocLengths2, start, numDocs, state);
					termVectorsWriter.AddRawDocuments(matchingVectorsReader, rawDocLengths, rawDocLengths2, numDocs, state);
					checkAbort.Work(300 * numDocs, state);
				}
			}
			else
			{
				for (int docNum = 0; docNum < maxDoc; docNum++)
				{
					if (reader.IsDeleted(docNum))
					{
						// skip deleted docs
						continue;
					}
					
					// NOTE: it's very important to first assign to vectors then pass it to
					// termVectorsWriter.addAllDocVectors; see LUCENE-1282
					ITermFreqVector[] vectors = reader.GetTermFreqVectors(docNum, state);
					termVectorsWriter.AddAllDocVectors(vectors);
					checkAbort.Work(300, state);
				}
			}
		}
		
		private void  CopyVectorsNoDeletions(TermVectorsWriter termVectorsWriter, TermVectorsReader matchingVectorsReader, IndexReader reader, IState state)
		{
			int maxDoc = reader.MaxDoc;
			if (matchingVectorsReader != null)
			{
				// We can bulk-copy because the fieldInfos are "congruent"
				int docCount = 0;
				while (docCount < maxDoc)
				{
					int len = System.Math.Min(MAX_RAW_MERGE_DOCS, maxDoc - docCount);
					matchingVectorsReader.RawDocs(rawDocLengths, rawDocLengths2, docCount, len, state);
					termVectorsWriter.AddRawDocuments(matchingVectorsReader, rawDocLengths, rawDocLengths2, len, state);
					docCount += len;
					checkAbort.Work(300 * len, state);
				}
			}
			else
			{
				for (int docNum = 0; docNum < maxDoc; docNum++)
				{
					// NOTE: it's very important to first assign to vectors then pass it to
					// termVectorsWriter.addAllDocVectors; see LUCENE-1282
					ITermFreqVector[] vectors = reader.GetTermFreqVectors(docNum, state);
					termVectorsWriter.AddAllDocVectors(vectors);
					checkAbort.Work(300, state);
				}
			}
		}
		
		private SegmentMergeQueue queue = null;
		
		private void  MergeTerms(IState s)
		{
			
			SegmentWriteState state = new SegmentWriteState(null, directory, segment, null, mergedDocs, 0, termIndexInterval);
			
			FormatPostingsFieldsConsumer consumer = new FormatPostingsFieldsWriter(state, fieldInfos, s);
			
			try
			{
				queue = new SegmentMergeQueue(readers.Count);
				
				MergeTermInfos(consumer, s);
			}
			finally
			{
				consumer.Finish();
				if (queue != null)
					queue.Dispose();
			}
		}
		
		internal bool omitTermFreqAndPositions;
		
		private void  MergeTermInfos(FormatPostingsFieldsConsumer consumer, IState state)
		{
			int base_Renamed = 0;
			int readerCount = readers.Count;
			for (int i = 0; i < readerCount; i++)
			{
				IndexReader reader = readers[i];
				TermEnum termEnum = reader.Terms(state);
				SegmentMergeInfo smi = new SegmentMergeInfo(base_Renamed, termEnum, reader);
				int[] docMap = smi.GetDocMap();
				if (docMap != null)
				{
					if (docMaps == null)
					{
						docMaps = new int[readerCount][];
						delCounts = new int[readerCount];
					}
					docMaps[i] = docMap;
					delCounts[i] = smi.reader.MaxDoc - smi.reader.NumDocs();
				}
				
				base_Renamed += reader.NumDocs();
				
				System.Diagnostics.Debug.Assert(reader.NumDocs() == reader.MaxDoc - smi.delCount);
				
				if (smi.Next(state))
					queue.Add(smi);
				// initialize queue
				else
					smi.Dispose();
			}
			
			SegmentMergeInfo[] match = new SegmentMergeInfo[readers.Count];
			
			System.String currentField = null;
			FormatPostingsTermsConsumer termsConsumer = null;
			
			while (queue.Size() > 0)
			{
				int matchSize = 0; // pop matching terms
				match[matchSize++] = queue.Pop();
				Term term = match[0].term;
				SegmentMergeInfo top = queue.Top();
				
				while (top != null && term.CompareTo(top.term) == 0)
				{
					match[matchSize++] = queue.Pop();
					top = queue.Top();
				}
				
				if ((System.Object) currentField != (System.Object) term.Field)
				{
                    currentField = term.Field;
					if (termsConsumer != null)
						termsConsumer.Finish();
					FieldInfo fieldInfo = fieldInfos.FieldInfo(currentField);
					termsConsumer = consumer.AddField(fieldInfo);
					omitTermFreqAndPositions = fieldInfo.omitTermFreqAndPositions;
				}
				
				int df = AppendPostings(termsConsumer, match, matchSize, state); // add new TermInfo
				
				checkAbort.Work(df / 3.0, state);
				
				while (matchSize > 0)
				{
					SegmentMergeInfo smi = match[--matchSize];
					if (smi.Next(state))
						queue.Add(smi);
					// restore queue
					else
						smi.Dispose(); // done with a segment
				}
			}
		}
		
		private byte[] payloadBuffer;
		private int[][] docMaps;
		internal int[][] GetDocMaps()
		{
			return docMaps;
		}
		private int[] delCounts;
		internal int[] GetDelCounts()
		{
			return delCounts;
		}
		
		/// <summary>Process postings from multiple segments all positioned on the
		/// same term. Writes out merged entries into freqOutput and
		/// the proxOutput streams.
		/// 
		/// </summary>
		/// <param name="smis">array of segments
		/// </param>
		/// <param name="n">number of cells in the array actually occupied
		/// </param>
		/// <returns> number of documents across all segments where this term was found
		/// </returns>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		private int AppendPostings(FormatPostingsTermsConsumer termsConsumer, SegmentMergeInfo[] smis, int n, IState state)
		{
			
			FormatPostingsDocsConsumer docConsumer = termsConsumer.AddTerm(smis[0].term.Text);
			int df = 0;
			for (int i = 0; i < n; i++)
			{
				SegmentMergeInfo smi = smis[i];
				TermPositions postings = smi.GetPositions(state);
				System.Diagnostics.Debug.Assert(postings != null);
				int base_Renamed = smi.base_Renamed;
				int[] docMap = smi.GetDocMap();
				postings.Seek(smi.termEnum, state);
				
				while (postings.Next(state))
				{
					df++;
					int doc = postings.Doc;
					if (docMap != null)
						doc = docMap[doc]; // map around deletions
					doc += base_Renamed; // convert to merged space
					
					int freq = postings.Freq;
					FormatPostingsPositionsConsumer posConsumer = docConsumer.AddDoc(doc, freq);
					
					if (!omitTermFreqAndPositions)
					{
						for (int j = 0; j < freq; j++)
						{
							int position = postings.NextPosition(state);
							int payloadLength = postings.PayloadLength;
							if (payloadLength > 0)
							{
								if (payloadBuffer == null || payloadBuffer.Length < payloadLength)
									payloadBuffer = new byte[payloadLength];
								postings.GetPayload(payloadBuffer, 0, state);
							}
							posConsumer.AddPosition(position, payloadBuffer, 0, payloadLength);
						}
						posConsumer.Finish();
					}
				}
			}
			docConsumer.Finish();
			
			return df;
		}
		
		private void  MergeNorms(IState state)
		{
			byte[] normBuffer = null;
			IndexOutput output = null;
			try
			{
				int numFieldInfos = fieldInfos.Size();
				for (int i = 0; i < numFieldInfos; i++)
				{
					FieldInfo fi = fieldInfos.FieldInfo(i);
					if (fi.isIndexed && !fi.omitNorms)
					{
						if (output == null)
						{
							output = directory.CreateOutput(segment + "." + IndexFileNames.NORMS_EXTENSION, state);
							output.WriteBytes(NORMS_HEADER, NORMS_HEADER.Length);
						}
						foreach(IndexReader reader in readers)
						{
							int maxDoc = reader.MaxDoc;
							if (normBuffer == null || normBuffer.Length < maxDoc)
							{
								// the buffer is too small for the current segment
								normBuffer = new byte[maxDoc];
							}
							reader.Norms(fi.name, normBuffer, 0, state);
							if (!reader.HasDeletions)
							{
								//optimized case for segments without deleted docs
								output.WriteBytes(normBuffer, maxDoc);
							}
							else
							{
								// this segment has deleted docs, so we have to
								// check for every doc if it is deleted or not
								for (int k = 0; k < maxDoc; k++)
								{
									if (!reader.IsDeleted(k))
									{
										output.WriteByte(normBuffer[k]);
									}
								}
							}
							checkAbort.Work(maxDoc, state);
						}
					}
				}
			}
			finally
			{
				if (output != null)
				{
					output.Close();
				}
			}
		}
		
		internal class CheckAbort
		{
			protected double workCount;
			protected MergePolicy.OneMerge merge;
			protected Directory dir;
			public CheckAbort(MergePolicy.OneMerge merge, Directory dir)
			{
				this.merge = merge;
				this.dir = dir;
			}
			
			/// <summary> Records the fact that roughly units amount of work
			/// have been done since this method was last called.
			/// When adding time-consuming code into SegmentMerger,
			/// you should test different values for units to ensure
			/// that the time in between calls to merge.checkAborted
			/// is up to ~ 1 second.
			/// </summary>
			public virtual void  Work(double units, IState state)
			{
				workCount += units;
				if (workCount >= 10000.0)
				{
					merge.CheckAborted(dir, state);
					workCount = 0;
				}
			}
		}
	}
}