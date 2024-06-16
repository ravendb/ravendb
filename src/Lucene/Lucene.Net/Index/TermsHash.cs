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
using ArrayUtil = Lucene.Net.Util.ArrayUtil;

namespace Lucene.Net.Index
{
	
	/// <summary>This class implements <see cref="InvertedDocConsumer" />, which
	/// is passed each token produced by the analyzer on each
	/// field.  It stores these tokens in a hash table, and
	/// allocates separate byte streams per token.  Consumers of
	/// this class, eg <see cref="FreqProxTermsWriter" /> and <see cref="TermVectorsTermsWriter" />
	///, write their own byte streams
	/// under each term.
	/// </summary>
	sealed class TermsHash : InvertedDocConsumer
	{
		
		internal TermsHashConsumer consumer;
		internal TermsHash nextTermsHash;
		internal int bytesPerPosting;
		internal int postingsFreeChunk;
		internal DocumentsWriter docWriter;
		private RawPostingList[] postingsFreeList = new RawPostingList[1];
		private int postingsFreeCount;
		private int postingsAllocCount;
		internal bool trackAllocations;
		
		public TermsHash(DocumentsWriter docWriter, bool trackAllocations, TermsHashConsumer consumer, TermsHash nextTermsHash)
		{
			this.docWriter = docWriter;
			this.consumer = consumer;
			this.nextTermsHash = nextTermsHash;
			this.trackAllocations = trackAllocations;
			
			// Why + 4*POINTER_NUM_BYTE below?
			//   +1: Posting is referenced by postingsFreeList array
			//   +3: Posting is referenced by hash, which
			//       targets 25-50% fill factor; approximate this
			//       as 3X # pointers
			bytesPerPosting = consumer.BytesPerPosting() + 4 * DocumentsWriter.POINTER_NUM_BYTE;
			postingsFreeChunk = (int) (DocumentsWriter.BYTE_BLOCK_SIZE / bytesPerPosting);
		}
		
		internal override InvertedDocConsumerPerThread AddThread(DocInverterPerThread docInverterPerThread)
		{
			return new TermsHashPerThread(docInverterPerThread, this, nextTermsHash, null);
		}
		
		internal TermsHashPerThread AddThread(DocInverterPerThread docInverterPerThread, TermsHashPerThread primaryPerThread)
		{
			return new TermsHashPerThread(docInverterPerThread, this, nextTermsHash, primaryPerThread);
		}
		
		internal override void  SetFieldInfos(FieldInfos fieldInfos)
		{
			this.fieldInfos = fieldInfos;
			consumer.SetFieldInfos(fieldInfos);
		}

        // NOTE: do not make this sync'd; it's not necessary (DW
        // ensures all other threads are idle), and it leads to
        // deadlock
		public override void  Abort()
		{
			consumer.Abort();
			if (nextTermsHash != null)
				nextTermsHash.Abort();
		}
		
		internal void  ShrinkFreePostings(IDictionary<InvertedDocConsumerPerThread, ICollection<InvertedDocConsumerPerField>> threadsAndFields, SegmentWriteState state)
		{
			
			System.Diagnostics.Debug.Assert(postingsFreeCount == postingsAllocCount, "Thread.currentThread().getName()" + ": postingsFreeCount=" + postingsFreeCount + " postingsAllocCount=" + postingsAllocCount + " consumer=" + consumer);

            int newSize = 1;
			if (newSize != postingsFreeList.Length)
			{
                if (postingsFreeCount > newSize)
                {
                    if (trackAllocations)
                    {
                        docWriter.BytesAllocated(-(postingsFreeCount - newSize) * bytesPerPosting);
                    }
                    postingsFreeCount = newSize;
                    postingsAllocCount = newSize;
                }

				RawPostingList[] newArray = new RawPostingList[newSize];
				Array.Copy(postingsFreeList, 0, newArray, 0, postingsFreeCount);
				postingsFreeList = newArray;
			}
		}
		
		internal override void  CloseDocStore(SegmentWriteState state, IState s)
		{
			lock (this)
			{
				consumer.CloseDocStore(state, s);
				if (nextTermsHash != null)
					nextTermsHash.CloseDocStore(state, s);
			}
		}
		
		internal override void  Flush(IDictionary<InvertedDocConsumerPerThread, ICollection<InvertedDocConsumerPerField>> threadsAndFields, SegmentWriteState state, IState s)
		{
			lock (this)
			{
                var childThreadsAndFields = new Dictionary<TermsHashConsumerPerThread, ICollection<TermsHashConsumerPerField>>();
                Dictionary<InvertedDocConsumerPerThread, ICollection<InvertedDocConsumerPerField>> nextThreadsAndFields;
				
				if (nextTermsHash != null)
				{
                    nextThreadsAndFields = new Dictionary<InvertedDocConsumerPerThread, ICollection<InvertedDocConsumerPerField>>();
				}
				else
					nextThreadsAndFields = null;

                foreach (var entry in threadsAndFields)
				{
					TermsHashPerThread perThread = (TermsHashPerThread) entry.Key;
					
					ICollection<InvertedDocConsumerPerField> fields = entry.Value;
					
					var fieldsIt = fields.GetEnumerator();
                    ICollection<TermsHashConsumerPerField> childFields = new HashSet<TermsHashConsumerPerField>();
					ICollection<InvertedDocConsumerPerField> nextChildFields;
					
					if (nextTermsHash != null)
					{
                        nextChildFields = new HashSet<InvertedDocConsumerPerField>();
					}
					else
						nextChildFields = null;
					
					while (fieldsIt.MoveNext())
					{
						TermsHashPerField perField = (TermsHashPerField) fieldsIt.Current;
						childFields.Add(perField.consumer);
						if (nextTermsHash != null)
							nextChildFields.Add(perField.nextPerField);
					}
					
					childThreadsAndFields[perThread.consumer] = childFields;
					if (nextTermsHash != null)
						nextThreadsAndFields[perThread.nextPerThread] = nextChildFields;
				}
				
				consumer.Flush(childThreadsAndFields, state, s);
				
				ShrinkFreePostings(threadsAndFields, state);
				
				if (nextTermsHash != null)
					nextTermsHash.Flush(nextThreadsAndFields, state, s);
			}
		}
		
		public override bool FreeRAM()
		{
			if (!trackAllocations)
				return false;
				
			bool any;
			long bytesFreed = 0;
            lock (this)
            {
                int numToFree;
                if (postingsFreeCount >= postingsFreeChunk)
                    numToFree = postingsFreeChunk;
                else
                    numToFree = postingsFreeCount;
                any = numToFree > 0;
                if (any)
                {
                    for (int i = postingsFreeCount - numToFree; i < postingsFreeCount; i++)
                    {
                        postingsFreeList[i] = null;
                    }
                    //Arrays.fill(postingsFreeList, postingsFreeCount - numToFree, postingsFreeCount, null);
                    postingsFreeCount -= numToFree;
                    postingsAllocCount -= numToFree;
                    bytesFreed = -numToFree * bytesPerPosting;
                    any = true;
                }
            }

			if (any)
			{
                docWriter.BytesAllocated(bytesFreed);
			}
				
			if (nextTermsHash != null)
				any |= nextTermsHash.FreeRAM();
				
			return any;
		}
		
		public void  RecyclePostings(RawPostingList[] postings, int numPostings)
		{
			lock (this)
			{
				
				System.Diagnostics.Debug.Assert(postings.Length >= numPostings);
				
				// Move all Postings from this ThreadState back to our
				// free list.  We pre-allocated this array while we were
				// creating Postings to make sure it's large enough
				System.Diagnostics.Debug.Assert(postingsFreeCount + numPostings <= postingsFreeList.Length);
				Array.Copy(postings, 0, postingsFreeList, postingsFreeCount, numPostings);
				postingsFreeCount += numPostings;
			}
		}
		
		public void  GetPostings(RawPostingList[] postings)
		{
			lock (this)
			{
				
				System.Diagnostics.Debug.Assert(docWriter.writer.TestPoint("TermsHash.getPostings start"));
				
				System.Diagnostics.Debug.Assert(postingsFreeCount <= postingsFreeList.Length);
				System.Diagnostics.Debug.Assert(postingsFreeCount <= postingsAllocCount, "postingsFreeCount=" + postingsFreeCount + " postingsAllocCount=" + postingsAllocCount);
				
				int numToCopy;
				if (postingsFreeCount < postings.Length)
					numToCopy = postingsFreeCount;
				else
					numToCopy = postings.Length;
				int start = postingsFreeCount - numToCopy;
				System.Diagnostics.Debug.Assert(start >= 0);
				System.Diagnostics.Debug.Assert(start + numToCopy <= postingsFreeList.Length);
				System.Diagnostics.Debug.Assert(numToCopy <= postings.Length);
				Array.Copy(postingsFreeList, start, postings, 0, numToCopy);
				
				// Directly allocate the remainder if any
				if (numToCopy != postings.Length)
				{
					int extra = postings.Length - numToCopy;
					int newPostingsAllocCount = postingsAllocCount + extra;
					
					consumer.CreatePostings(postings, numToCopy, extra);
					System.Diagnostics.Debug.Assert(docWriter.writer.TestPoint("TermsHash.getPostings after create"));
					postingsAllocCount += extra;
					
					if (trackAllocations)
						docWriter.BytesAllocated(extra * bytesPerPosting);
					
					if (newPostingsAllocCount > postingsFreeList.Length)
					// Pre-allocate the postingsFreeList so it's large
					// enough to hold all postings we've given out
						postingsFreeList = new RawPostingList[ArrayUtil.GetNextSize(newPostingsAllocCount)];
				}
				
				postingsFreeCount -= numToCopy;
				
				if (trackAllocations)
					docWriter.BytesUsed(postings.Length * bytesPerPosting);
			}
		}
	}
}