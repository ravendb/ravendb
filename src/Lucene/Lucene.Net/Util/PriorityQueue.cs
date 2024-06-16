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
using Lucene.Net.Support;

namespace Lucene.Net.Util
{
	
	/// <summary>A PriorityQueue maintains a partial ordering of its elements such that the
	/// least element can always be found in constant time.  Put()'s and pop()'s
	/// require log(size) time.
	/// 
	/// <p/><b>NOTE</b>: This class pre-allocates a full array of
	/// length <c>maxSize+1</c>, in <see cref="Initialize" />.
	/// 
	/// </summary>
	// TODO: T needs to be able to return null.  Behavior might be unexpected otherwise, since it returns default(T)
    //       I only see a non-nullable type used in PriorityQueue in the tests.  may be possible to re-write tests to
    //       use an IComparable class, and this can be changed back to constraining on class, to return null, or should
    //       we leave as is?
	public abstract class PriorityQueue<T> //where T : class
	{
		private int size;
		private int maxSize;
		protected internal T[] heap;
		
		/// <summary>Determines the ordering of objects in this priority queue.  Subclasses
		/// must define this one method. 
		/// </summary>
		public abstract bool LessThan(T a, T b);

	    /// <summary> This method can be overridden by extending classes to return a sentinel
	    /// object which will be used by <see cref="Initialize(int)" /> to fill the queue, so
	    /// that the code which uses that queue can always assume it's full and only
	    /// change the top without attempting to insert any new object.<br/>
	    /// 
	    /// Those sentinel values should always compare worse than any non-sentinel
	    /// value (i.e., <see cref="LessThan" /> should always favor the
	    /// non-sentinel values).<br/>
	    /// 
	    /// By default, this method returns false, which means the queue will not be
	    /// filled with sentinel values. Otherwise, the value returned will be used to
	    /// pre-populate the queue. Adds sentinel values to the queue.<br/>
	    /// 
	    /// If this method is extended to return a non-null value, then the following
	    /// usage pattern is recommended:
	    /// 
	    /// <code>
	    /// // extends getSentinelObject() to return a non-null value.
	    /// PriorityQueue&lt;MyObject&gt; pq = new MyQueue&lt;MyObject&gt;(numHits);
	    /// // save the 'top' element, which is guaranteed to not be null.
	    /// MyObject pqTop = pq.top();
	    /// &lt;...&gt;
	    /// // now in order to add a new element, which is 'better' than top (after 
	    /// // you've verified it is better), it is as simple as:
	    /// pqTop.change().
	    /// pqTop = pq.updateTop();
	    /// </code>
	    /// 
	    /// <b>NOTE:</b> if this method returns a non-null value, it will be called by
	    /// <see cref="Initialize(int)" /> <see cref="Size()" /> times, relying on a new object to
	    /// be returned and will not check if it's null again. Therefore you should
	    /// ensure any call to this method creates a new instance and behaves
	    /// consistently, e.g., it cannot return null if it previously returned
	    /// non-null.
	    /// 
	    /// </summary>
	    /// <value> the sentinel object to use to pre-populate the queue, or null if sentinel objects are not supported. </value>
	    protected internal virtual T SentinelObject
	    {
	        get { return default(T); }
	    }

	    /// <summary>Subclass constructors must call this. </summary>
		protected internal void  Initialize(int maxSize)
		{
			size = 0;
			int heapSize;
            if (0 == maxSize)
                // We allocate 1 extra to avoid if statement in top()
                heapSize = 2;
            else
            {
                if (maxSize == Int32.MaxValue)
                {
                    // Don't wrap heapSize to -1, in this case, which
                    // causes a confusing NegativeArraySizeException.
                    // Note that very likely this will simply then hit
                    // an OOME, but at least that's more indicative to
                    // caller that this values is too big.  We don't +1
                    // in this case, but it's very unlikely in practice
                    // one will actually insert this many objects into
                    // the PQ:
                    heapSize = Int32.MaxValue;
                }
                else
                {
                    // NOTE: we add +1 because all access to heap is
                    // 1-based not 0-based.  heap[0] is unused.
                    heapSize = maxSize + 1;
                }
            }
			heap = new T[heapSize];
			this.maxSize = maxSize;
			
			// If sentinel objects are supported, populate the queue with them
			T sentinel = SentinelObject;
			if (sentinel != null)
			{
				heap[1] = sentinel;
				for (int i = 2; i < heap.Length; i++)
				{
					heap[i] = SentinelObject;
				}
				size = maxSize;
			}
		}
		
		/// <summary> 
		/// Adds an Object to a PriorityQueue in log(size) time. If one tries to add
		/// more objects than maxSize from initialize an
		/// <see cref="System.IndexOutOfRangeException" /> is thrown.
		/// </summary>
		/// <returns> the new 'top' element in the queue.
		/// </returns>
		public T Add(T element)
		{
			size++;
			heap[size] = element;
			UpHeap();
			return heap[1];
		}

        /// <summary> Adds an Object to a PriorityQueue in log(size) time.
        /// It returns the object (if any) that was
		/// dropped off the heap because it was full. This can be
		/// the given parameter (in case it is smaller than the
		/// full heap's minimum, and couldn't be added), or another
		/// object that was previously the smallest value in the
		/// heap and now has been replaced by a larger one, or null
		/// if the queue wasn't yet full with maxSize elements.
		/// </summary>
		public virtual T InsertWithOverflow(T element)
		{
			if (size < maxSize)
			{
				Add(element);
				return default(T);
			}
			else if (size > 0 && !LessThan(element, heap[1]))
			{
				T ret = heap[1];
				heap[1] = element;
				UpdateTop();
				return ret;
			}
			else
			{
				return element;
			}
		}
		
		/// <summary>Returns the least element of the PriorityQueue in constant time. </summary>
		public T Top()
		{
			// We don't need to check size here: if maxSize is 0,
			// then heap is length 2 array with both entries null.
			// If size is 0 then heap[1] is already null.
			return heap[1];
		}
		
		/// <summary>
		/// Removes and returns the least element of the 
		/// PriorityQueue in log(size) time. 
		/// </summary>
		public T Pop()
		{
			if (size > 0)
			{
				T result = heap[1]; // save first value
				heap[1] = heap[size]; // move last to first
				heap[size] = default(T); // permit GC of objects
				size--;
				DownHeap(); // adjust heap
				return result;
			}
			else
                return default(T);
		}
		
		/// <summary> Should be called when the Object at top changes values. 
		/// Still log(n) worst case, but it's at least twice as fast to
        /// <code>
		/// pq.top().change();
		/// pq.updateTop();
        /// </code>
		/// instead of
        /// <code>
		/// o = pq.pop();
		/// o.change();
		/// pq.push(o);
        /// </code>
		/// </summary>
		/// <returns> the new 'top' element.</returns>
		public T UpdateTop()
		{
			DownHeap();
			return heap[1];
		}
		
		/// <summary>Returns the number of elements currently stored in the PriorityQueue. </summary>
		public int Size()
		{
			return size;
		}
		
		/// <summary>Removes all entries from the PriorityQueue. </summary>
		public void  Clear()
		{
			for (int i = 0; i <= size; i++)
			{
                heap[i] = default(T);
			}
			size = 0;
		}
		
		private void  UpHeap()
		{
			int i = size;
			T node = heap[i]; // save bottom node
			int j = Number.URShift(i, 1);
			while (j > 0 && LessThan(node, heap[j]))
			{
				heap[i] = heap[j]; // shift parents down
				i = j;
				j = Number.URShift(j, 1);
			}
			heap[i] = node; // install saved node
		}
		
		private void  DownHeap()
		{
			int i = 1;
			T node = heap[i]; // save top node
			int j = i << 1; // find smaller child
			int k = j + 1;
			if (k <= size && LessThan(heap[k], heap[j]))
			{
				j = k;
			}
			while (j <= size && LessThan(heap[j], node))
			{
				heap[i] = heap[j]; // shift up child
				i = j;
				j = i << 1;
				k = j + 1;
				if (k <= size && LessThan(heap[k], heap[j]))
				{
					j = k;
				}
			}
			heap[i] = node; // install saved node
		}
	}
}