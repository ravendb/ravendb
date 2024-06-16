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

namespace Lucene.Net.Search
{

    /// <summary> A DocIdSet contains a set of doc ids. Implementing classes must
    /// only implement <see cref="Iterator" /> to provide access to the set. 
    /// </summary>
    [Serializable]
    public abstract class DocIdSet
	{
		public class AnonymousClassDocIdSet:DocIdSet
		{
			public AnonymousClassDocIdSet()
			{
				InitBlock();
			}
			public class AnonymousClassDocIdSetIterator:DocIdSetIterator
			{
				public AnonymousClassDocIdSetIterator(AnonymousClassDocIdSet enclosingInstance)
				{
					InitBlock(enclosingInstance);
				}
				private void  InitBlock(AnonymousClassDocIdSet enclosingInstance)
				{
					this.enclosingInstance = enclosingInstance;
				}
				private AnonymousClassDocIdSet enclosingInstance;
				public AnonymousClassDocIdSet Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				public override int Advance(int target, IState state)
				{
					return NO_MORE_DOCS;
				}
				public override int DocID()
				{
					return NO_MORE_DOCS;
				}
				public override int NextDoc(IState state)
				{
					return NO_MORE_DOCS;
				}
			}
			private void  InitBlock()
			{
				iterator = new AnonymousClassDocIdSetIterator(this);
			}
			
			private DocIdSetIterator iterator;
			
			public override DocIdSetIterator Iterator(IState state)
			{
				return iterator;
			}

		    public override bool IsCacheable
		    {
		        get { return true; }
		    }
		}

        /// <summary>An empty <see cref="DocIdSet"/> instance for easy use, e.g. in Filters that hit no documents. </summary>
		[NonSerialized]
		public static readonly DocIdSet EMPTY_DOCIDSET;
		
		/// <summary>Provides a <see cref="DocIdSetIterator" /> to access the set.
		/// This implementation can return <c>null</c> or
		/// <c>EMPTY_DOCIDSET.Iterator()</c> if there
		/// are no docs that match. 
		/// </summary>
		public abstract DocIdSetIterator Iterator(IState state);

	    /// <summary>This method is a hint for <see cref="CachingWrapperFilter" />, if this <c>DocIdSet</c>
	    /// should be cached without copying it into a BitSet. The default is to return
	    /// <c>false</c>. If you have an own <c>DocIdSet</c> implementation
	    /// that does its iteration very effective and fast without doing disk I/O,
	    /// override this method and return true.
	    /// </summary>
	    public virtual bool IsCacheable
	    {
	        get { return false; }
	    }

	    static DocIdSet()
		{
			EMPTY_DOCIDSET = new AnonymousClassDocIdSet();
		}
	}
}