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
using Lucene.Net.Util;

namespace Lucene.Net.Index
{
	
	/// <summary> Allows you to iterate over the <see cref="TermPositions" /> for multiple <see cref="Term" />s as
	/// a single <see cref="TermPositions" />.
	/// 
	/// </summary>
	public class MultipleTermPositions : TermPositions
	{
		private sealed class TermPositionsQueue : PriorityQueue<TermPositions>
		{
			internal TermPositionsQueue(LinkedList<TermPositions> termPositions, IState state)
			{
				Initialize(termPositions.Count);
				
				foreach(TermPositions tp in termPositions)
					if (tp.Next(state))
						Add(tp);
			}
			
			internal TermPositions Peek()
			{
				return Top();
			}
			
			public override bool LessThan(TermPositions a, TermPositions b)
			{
				return a.Doc < b.Doc;
			}
		}
		
		private sealed class IntQueue
		{
			public IntQueue()
			{
				InitBlock();
			}
			private void  InitBlock()
			{
				_array = new int[_arraySize];
			}
			private int _arraySize = 16;
			private int _index = 0;
			private int _lastIndex = 0;
			private int[] _array;
			
			internal void  add(int i)
			{
				if (_lastIndex == _arraySize)
					growArray();
				
				_array[_lastIndex++] = i;
			}
			
			internal int next()
			{
				return _array[_index++];
			}
			
			internal void  sort()
			{
				System.Array.Sort(_array, _index, _lastIndex - _index);
			}
			
			internal void  clear()
			{
				_index = 0;
				_lastIndex = 0;
			}
			
			internal int size()
			{
				return (_lastIndex - _index);
			}
			
			private void  growArray()
			{
				int[] newArray = new int[_arraySize * 2];
				Array.Copy(_array, 0, newArray, 0, _arraySize);
				_array = newArray;
				_arraySize *= 2;
			}
		}
		
		private int _doc;
		private int _freq;
		private TermPositionsQueue _termPositionsQueue;
		private IntQueue _posList;

	    private bool isDisposed;
		/// <summary> Creates a new <c>MultipleTermPositions</c> instance.
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException">
		/// </exception>
		public MultipleTermPositions(IndexReader indexReader, Term[] terms, IState state)
		{
			var termPositions = new System.Collections.Generic.LinkedList<TermPositions>();
			
			for (int i = 0; i < terms.Length; i++)
				termPositions.AddLast(indexReader.TermPositions(terms[i], state));
			
			_termPositionsQueue = new TermPositionsQueue(termPositions, state);
			_posList = new IntQueue();
		}
		
		public bool Next(IState state)
		{
			if (_termPositionsQueue.Size() == 0)
				return false;
			
			_posList.clear();
			_doc = _termPositionsQueue.Peek().Doc;
			
			TermPositions tp;
			do 
			{
				tp = _termPositionsQueue.Peek();
				
				for (int i = 0; i < tp.Freq; i++)
					_posList.add(tp.NextPosition(state));
				
				if (tp.Next(state))
					_termPositionsQueue.UpdateTop();
				else
				{
					_termPositionsQueue.Pop();
					tp.Close();
				}
			}
			while (_termPositionsQueue.Size() > 0 && _termPositionsQueue.Peek().Doc == _doc);
			
			_posList.sort();
			_freq = _posList.size();
			
			return true;
		}
		
		public int NextPosition(IState state)
		{
			return _posList.next();
		}
		
		public bool SkipTo(int target, IState state)
		{
			while (_termPositionsQueue.Peek() != null && target > _termPositionsQueue.Peek().Doc)
			{
				TermPositions tp = _termPositionsQueue.Pop();
				if (tp.SkipTo(target, state))
					_termPositionsQueue.Add(tp);
				else
					tp.Close();
			}
			return Next(state);
		}

	    public int Doc
	    {
	        get { return _doc; }
	    }

	    public int Freq
	    {
	        get { return _freq; }
	    }

	    [Obsolete("Use Dispose() instead")]
		public void  Close()
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
                while (_termPositionsQueue.Size() > 0)
                    _termPositionsQueue.Pop().Close();
            }

            isDisposed = true;
        }
		
		/// <summary> Not implemented.</summary>
		/// <throws>  UnsupportedOperationException </throws>
		public virtual void Seek(Term arg0, IState state)
		{
			throw new System.NotSupportedException();
		}
		
		/// <summary> Not implemented.</summary>
		/// <throws>  UnsupportedOperationException </throws>
		public virtual void Seek(TermEnum termEnum, IState state)
		{
			throw new System.NotSupportedException();
		}
		
		/// <summary> Not implemented.</summary>
		/// <throws>  UnsupportedOperationException </throws>
		public virtual int Read(Span<int> arg0, Span<int> arg1, IState state)
		{
			throw new System.NotSupportedException();
		}


	    /// <summary> Not implemented.</summary>
	    /// <throws>  UnsupportedOperationException </throws>
	    public virtual int PayloadLength
	    {
	        get { throw new System.NotSupportedException(); }
	    }

	    /// <summary> Not implemented.</summary>
		/// <throws>  UnsupportedOperationException </throws>
		public virtual byte[] GetPayload(byte[] data, int offset, IState state)
		{
			throw new System.NotSupportedException();
		}

	    /// <summary> </summary>
	    /// <value> false </value>
// TODO: Remove warning after API has been finalized
	    public virtual bool IsPayloadAvailable
	    {
	        get { return false; }
	    }
	}
}