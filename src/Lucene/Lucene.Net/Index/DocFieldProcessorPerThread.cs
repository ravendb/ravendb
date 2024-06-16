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
using Lucene.Net.Documents;
using Lucene.Net.Store;
using Lucene.Net.Support;
using Document = Lucene.Net.Documents.Document;
using ArrayUtil = Lucene.Net.Util.ArrayUtil;
using System.Runtime.CompilerServices;
using Lucene.Net.Util;

namespace Lucene.Net.Index
{
	
	/// <summary> Gathers all Fieldables for a document under the same
	/// name, updates FieldInfos, and calls per-field consumers
	/// to process field by field.
	/// 
	/// Currently, only a single thread visits the fields,
	/// sequentially, for processing.
	/// </summary>
	
	sealed class DocFieldProcessorPerThread : DocConsumerPerThread
	{
		private void  InitBlock()
		{
			docFreeList = new PerDoc[1];
		}
		
		internal float docBoost;
		internal int fieldGen;
		internal DocFieldProcessor docFieldProcessor;
		internal FieldInfos fieldInfos;
		internal DocFieldConsumerPerThread consumer;
		
		// Holds all fields seen in current doc
		internal DocFieldProcessorPerField[] fields = new DocFieldProcessorPerField[1];
		internal int fieldCount;
		
		// Hash table for all fields ever seen
		internal DocFieldProcessorPerField[] fieldHash = new DocFieldProcessorPerField[2];
		internal int hashMask = 1;
		internal int totalFieldCount;
		
		internal StoredFieldsWriterPerThread fieldsWriter;
		
		internal DocumentsWriter.DocState docState;

	    private readonly Sorter<DocFieldProcessorPerField, DocFieldProcessorPerFieldComparer> _sorter = default(Sorter<DocFieldProcessorPerField, DocFieldProcessorPerFieldComparer>);

        public DocFieldProcessorPerThread(DocumentsWriterThreadState threadState, DocFieldProcessor docFieldProcessor)
		{
			InitBlock();
			this.docState = threadState.docState;
			this.docFieldProcessor = docFieldProcessor;
			this.fieldInfos = docFieldProcessor.fieldInfos;
			this.consumer = docFieldProcessor.consumer.AddThread(this);
			fieldsWriter = docFieldProcessor.fieldsWriter.AddThread(docState);
		}
		
		public override void  Abort()
		{
			for (int i = 0; i < fieldHash.Length; i++)
			{
				DocFieldProcessorPerField field = fieldHash[i];
				while (field != null)
				{
					DocFieldProcessorPerField next = field.next;
					field.Abort();
					field = next;
				}
			}
			fieldsWriter.Abort();
			consumer.Abort();
		}
		
		public System.Collections.Generic.ICollection<DocFieldConsumerPerField> Fields()
		{
		    System.Collections.Generic.ICollection<DocFieldConsumerPerField> fields =
		        new System.Collections.Generic.HashSet<DocFieldConsumerPerField>();
			for (int i = 0; i < fieldHash.Length; i++)
			{
				DocFieldProcessorPerField field = fieldHash[i];
				while (field != null)
				{
					fields.Add(field.consumer);
					field = field.next;
				}
			}
			System.Diagnostics.Debug.Assert(fields.Count == totalFieldCount);
			return fields;
		}
		
		/// <summary>If there are fields we've seen but did not see again
		/// in the last run, then free them up. 
		/// </summary>
		
		internal void  TrimFields(SegmentWriteState state)
		{
			
			for (int i = 0; i < fieldHash.Length; i++)
			{
				DocFieldProcessorPerField perField = fieldHash[i];
				DocFieldProcessorPerField lastPerField = null;
				
				while (perField != null)
				{
					
					if (perField.lastGen == - 1)
					{
						
						// This field was not seen since the previous
						// flush, so, free up its resources now
						
						// Unhash
						if (lastPerField == null)
							fieldHash[i] = perField.next;
						else
							lastPerField.next = perField.next;
						
						if (state.docWriter.infoStream != null)
							state.docWriter.infoStream.WriteLine("  purge field=" + perField.fieldInfo.name);
						
						totalFieldCount--;
					}
					else
					{
						// Reset
						perField.lastGen = - 1;
						lastPerField = perField;
					}
					
					perField = perField.next;
				}
			}
		}
		
		private void  Rehash()
		{
			int newHashSize = (fieldHash.Length * 2);
			System.Diagnostics.Debug.Assert(newHashSize > fieldHash.Length);
			
			DocFieldProcessorPerField[] newHashArray = new DocFieldProcessorPerField[newHashSize];
			
			// Rehash
			int newHashMask = newHashSize - 1;
			for (int j = 0; j < fieldHash.Length; j++)
			{
				DocFieldProcessorPerField fp0 = fieldHash[j];
				while (fp0 != null)
				{
					int hashPos2 = fp0.fieldInfo.name.GetHashCode() & newHashMask;
					DocFieldProcessorPerField nextFP0 = fp0.next;
					fp0.next = newHashArray[hashPos2];
					newHashArray[hashPos2] = fp0;
					fp0 = nextFP0;
				}
			}
			
			fieldHash = newHashArray;
			hashMask = newHashMask;
		}
		
		public override DocumentsWriter.DocWriter ProcessDocument(IState state)
		{
			
			consumer.StartDocument();
			fieldsWriter.StartDocument();
			
			Document doc = docState.doc;
			
			System.Diagnostics.Debug.Assert(docFieldProcessor.docWriter.writer.TestPoint("DocumentsWriter.ThreadState.init start"));
			
			fieldCount = 0;
			
			int thisFieldGen = fieldGen++;
			
			System.Collections.Generic.IList<IFieldable> docFields = doc.GetFields();
			int numDocFields = docFields.Count;
			
			// Absorb any new fields first seen in this document.
			// Also absorb any changes to fields we had already
			// seen before (eg suddenly turning on norms or
			// vectors, etc.):
			
			for (int i = 0; i < numDocFields; i++)
			{
				IFieldable field = docFields[i];
				System.String fieldName = field.Name;
				
				// Make sure we have a PerField allocated
				int hashPos = fieldName.GetHashCode() & hashMask;
				DocFieldProcessorPerField fp = fieldHash[hashPos];
				while (fp != null && !fp.fieldInfo.name.Equals(fieldName))
					fp = fp.next;

                if (fp == null)
                {

                    // TODO FI: we need to genericize the "flags" that a
                    // field holds, and, how these flags are merged; it
                    // needs to be more "pluggable" such that if I want
                    // to have a new "thing" my Fields can do, I can
                    // easily add it
                    FieldInfo fi = fieldInfos.Add(fieldName, field.IsIndexed, field.IsTermVectorStored,
                                                  field.IsStorePositionWithTermVector, field.IsStoreOffsetWithTermVector,
                                                  field.OmitNorms, false, field.OmitTermFreqAndPositions);

                    fp = new DocFieldProcessorPerField(this, fi);
                    fp.next = fieldHash[hashPos];
                    fieldHash[hashPos] = fp;
                    totalFieldCount++;

                    if (totalFieldCount >= fieldHash.Length / 2)
                        Rehash();
                }
                else
                {
                    fp.fieldInfo.Update(field.IsIndexed, field.IsTermVectorStored,
                                        field.IsStorePositionWithTermVector, field.IsStoreOffsetWithTermVector,
                                        field.OmitNorms, false, field.OmitTermFreqAndPositions);
                }

			    if (thisFieldGen != fp.lastGen)
				{
					
					// First time we're seeing this field for this doc
					fp.fieldCount = 0;
					
					if (fieldCount == fields.Length)
					{
						int newSize = fields.Length * 2;
						DocFieldProcessorPerField[] newArray = new DocFieldProcessorPerField[newSize];
						Array.Copy(fields, 0, newArray, 0, fieldCount);
						fields = newArray;
					}
					
					fields[fieldCount++] = fp;
					fp.lastGen = thisFieldGen;
				}
				
				if (fp.fieldCount == fp.fields.Length)
				{
					IFieldable[] newArray = new IFieldable[fp.fields.Length * 2];
					Array.Copy(fp.fields, 0, newArray, 0, fp.fieldCount);
					fp.fields = newArray;
				}
				
				fp.fields[fp.fieldCount++] = field;
				if (field.IsStored)
				{
					fieldsWriter.AddField(field, fp.fieldInfo, state);
				}
			}

            // If we are writing vectors then we must visit
            // fields in sorted order so they are written in
            // sorted order.  TODO: we actually only need to
            // sort the subset of fields that have vectors
            // enabled; we could save [small amount of] CPU
            // here.

		    _sorter.Sort(fields, 0, fieldCount);
            // QuickSort(fields, 0, fieldCount - 1);
			
			for (int i = 0; i < fieldCount; i++)
				fields[i].consumer.ProcessFields(fields[i].fields, fields[i].fieldCount, state);

            if (docState.maxTermPrefix != null && docState.infoStream != null)
            {
                docState.infoStream.WriteLine("WARNING: document contains at least one immense term (longer than the max length " + DocumentsWriter.MAX_TERM_LENGTH + "), all of which were skipped.  Please correct the analyzer to not produce such terms.  The prefix of the first immense term is: '" + docState.maxTermPrefix + "...'");
                docState.maxTermPrefix = null;
            }
			
			DocumentsWriter.DocWriter one = fieldsWriter.FinishDocument();
			DocumentsWriter.DocWriter two = consumer.FinishDocument();
			if (one == null)
			{
				return two;
			}
			else if (two == null)
			{
				return one;
			}
			else
			{
				PerDoc both = GetPerDoc();
				both.docID = docState.docID;
				System.Diagnostics.Debug.Assert(one.docID == docState.docID);
				System.Diagnostics.Debug.Assert(two.docID == docState.docID);
				both.one = one;
				both.two = two;
				return both;
			}
		}

	    private struct DocFieldProcessorPerFieldComparer : IComparer<DocFieldProcessorPerField>
	    {
	        [MethodImpl(MethodImplOptions.AggressiveInlining)]
	        public int Compare(DocFieldProcessorPerField x, DocFieldProcessorPerField y)
	        {
	            return String.CompareOrdinal(x.fieldInfo.name, y.fieldInfo.name);
	        }
	    } 
		
		internal PerDoc[] docFreeList;
		internal int freeCount;
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
		
		internal void  FreePerDoc(PerDoc perDoc)
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(freeCount < docFreeList.Length);
				docFreeList[freeCount++] = perDoc;
			}
		}
		
		internal class PerDoc:DocumentsWriter.DocWriter
		{
			public PerDoc(DocFieldProcessorPerThread enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(DocFieldProcessorPerThread enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private DocFieldProcessorPerThread enclosingInstance;
			public DocFieldProcessorPerThread Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			internal DocumentsWriter.DocWriter one;
			internal DocumentsWriter.DocWriter two;
			
			public override long SizeInBytes()
			{
				return one.SizeInBytes() + two.SizeInBytes();
			}
			
			public override void  Finish(IState state)
			{
				try
				{
					try
					{
						one.Finish(state);
					}
					finally
					{
						two.Finish(state);
					}
				}
				finally
				{
					Enclosing_Instance.FreePerDoc(this);
				}
			}
			
			public override void  Abort()
			{
				try
				{
					try
					{
						one.Abort();
					}
					finally
					{
						two.Abort();
					}
				}
				finally
				{
					Enclosing_Instance.FreePerDoc(this);
				}
			}
		}
	}
}