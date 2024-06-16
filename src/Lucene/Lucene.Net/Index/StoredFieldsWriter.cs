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
using RAMOutputStream = Lucene.Net.Store.RAMOutputStream;
using ArrayUtil = Lucene.Net.Util.ArrayUtil;

namespace Lucene.Net.Index
{
	
	/// <summary>This is a DocFieldConsumer that writes stored fields. </summary>
	sealed class StoredFieldsWriter
	{
		private void  InitBlock()
		{
			docFreeList = new PerDoc[1];
		}
		
		internal FieldsWriter fieldsWriter;
		internal DocumentsWriter docWriter;
		internal FieldInfos fieldInfos;
		internal int lastDocID;
		
		internal PerDoc[] docFreeList;
		internal int freeCount;
		
		public StoredFieldsWriter(DocumentsWriter docWriter, FieldInfos fieldInfos)
		{
			InitBlock();
			this.docWriter = docWriter;
			this.fieldInfos = fieldInfos;
		}
		
		public StoredFieldsWriterPerThread AddThread(DocumentsWriter.DocState docState)
		{
			return new StoredFieldsWriterPerThread(docState, this);
		}
		
		public void  Flush(SegmentWriteState state, IState s)
		{
			lock (this)
			{
				
				if (state.numDocsInStore > 0)
				{
					// It's possible that all documents seen in this segment
					// hit non-aborting exceptions, in which case we will
					// not have yet init'd the FieldsWriter:
					InitFieldsWriter(s);
					
					// Fill fdx file to include any final docs that we
					// skipped because they hit non-aborting exceptions
					Fill(state.numDocsInStore - docWriter.DocStoreOffset);
				}
				
				if (fieldsWriter != null)
					fieldsWriter.Flush();
			}
		}
		
		private void  InitFieldsWriter(IState state)
		{
			if (fieldsWriter == null)
			{
				System.String docStoreSegment = docWriter.DocStoreSegment;
				if (docStoreSegment != null)
				{
					System.Diagnostics.Debug.Assert(docStoreSegment != null);
					fieldsWriter = new FieldsWriter(docWriter.directory, docStoreSegment, fieldInfos, state);
					docWriter.AddOpenFile(docStoreSegment + "." + IndexFileNames.FIELDS_EXTENSION);
					docWriter.AddOpenFile(docStoreSegment + "." + IndexFileNames.FIELDS_INDEX_EXTENSION);
					lastDocID = 0;
				}
			}
		}
		
		public void  CloseDocStore(SegmentWriteState state, IState s)
		{
			lock (this)
			{
				int inc = state.numDocsInStore - lastDocID;
				if (inc > 0)
				{
					InitFieldsWriter(s);
					Fill(state.numDocsInStore - docWriter.DocStoreOffset);
				}
				
				if (fieldsWriter != null)
				{
					fieldsWriter.Dispose();
					fieldsWriter = null;
					lastDocID = 0;
					System.Diagnostics.Debug.Assert(state.docStoreSegmentName != null);
                    state.flushedFiles.Add(state.docStoreSegmentName + "." + IndexFileNames.FIELDS_EXTENSION);
                    state.flushedFiles.Add(state.docStoreSegmentName + "." + IndexFileNames.FIELDS_INDEX_EXTENSION);
					
					state.docWriter.RemoveOpenFile(state.docStoreSegmentName + "." + IndexFileNames.FIELDS_EXTENSION);
					state.docWriter.RemoveOpenFile(state.docStoreSegmentName + "." + IndexFileNames.FIELDS_INDEX_EXTENSION);
					
					System.String fileName = state.docStoreSegmentName + "." + IndexFileNames.FIELDS_INDEX_EXTENSION;
					
					if (4 + ((long) state.numDocsInStore) * 8 != state.directory.FileLength(fileName, s))
						throw new System.SystemException("after flush: fdx size mismatch: " + state.numDocsInStore + " docs vs " + state.directory.FileLength(fileName, s) + " length in bytes of " + fileName + " file exists?=" + state.directory.FileExists(fileName, s));
				}
			}
		}
		
		internal int allocCount;
		
		internal PerDoc GetPerDoc()
		{
			lock (this)
			{
				if (freeCount == 0)
				{
					allocCount++;
					if (allocCount > docFreeList.Length)
					{
						// Grow our free list up front to make sure we have
						// enough space to recycle all outstanding PerDoc
						// instances
						System.Diagnostics.Debug.Assert(allocCount == 1 + docFreeList.Length);
						docFreeList = new PerDoc[ArrayUtil.GetNextSize(allocCount)];
					}
					return new PerDoc(this);
				}
				else
					return docFreeList[--freeCount];
			}
		}
		
		internal void  Abort()
		{
			lock (this)
			{
				if (fieldsWriter != null)
				{
					try
					{
						fieldsWriter.Dispose();
					}
					catch (System.Exception)
					{
					}
					fieldsWriter = null;
					lastDocID = 0;
				}
			}
		}
		
		/// <summary>Fills in any hole in the docIDs </summary>
		internal void  Fill(int docID)
		{
			int docStoreOffset = docWriter.DocStoreOffset;
			
			// We must "catch up" for all docs before us
			// that had no stored fields:
			int end = docID + docStoreOffset;
			while (lastDocID < end)
			{
				fieldsWriter.SkipDocument();
				lastDocID++;
			}
		}
		
		internal void  FinishDocument(PerDoc perDoc, IState state)
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(docWriter.writer.TestPoint("StoredFieldsWriter.finishDocument start"));
				InitFieldsWriter(state);
				
				Fill(perDoc.docID);
				
				// Append stored fields to the real FieldsWriter:
				fieldsWriter.FlushDocument(perDoc.numStoredFields, perDoc.fdt);
				lastDocID++;
				perDoc.Reset();
				Free(perDoc);
				System.Diagnostics.Debug.Assert(docWriter.writer.TestPoint("StoredFieldsWriter.finishDocument end"));
			}
		}
		
		public bool FreeRAM()
		{
			return false;
		}
		
		internal void  Free(PerDoc perDoc)
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(freeCount < docFreeList.Length);
				System.Diagnostics.Debug.Assert(0 == perDoc.numStoredFields);
				System.Diagnostics.Debug.Assert(0 == perDoc.fdt.Length);
				System.Diagnostics.Debug.Assert(0 == perDoc.fdt.FilePointer);
				docFreeList[freeCount++] = perDoc;
			}
		}
		
		internal class PerDoc:DocumentsWriter.DocWriter
		{
			public PerDoc(StoredFieldsWriter enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(StoredFieldsWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
                buffer = enclosingInstance.docWriter.NewPerDocBuffer();
                fdt = new RAMOutputStream(buffer);
			}
			private StoredFieldsWriter enclosingInstance;
			public StoredFieldsWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}

            internal DocumentsWriter.PerDocBuffer buffer ;
            internal RAMOutputStream fdt;
			internal int numStoredFields;
			
			internal void  Reset()
			{
				fdt.Reset();
                buffer.Recycle();
				numStoredFields = 0;
			}
			
			public override void  Abort()
			{
				Reset();
				Enclosing_Instance.Free(this);
			}
			
			public override long SizeInBytes()
			{
                return buffer.SizeInBytes;
			}
			
			public override void  Finish(IState state)
			{
				Enclosing_Instance.FinishDocument(this, state);
			}
		}
	}
}