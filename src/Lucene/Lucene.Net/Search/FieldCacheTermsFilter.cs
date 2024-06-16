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
using IndexReader = Lucene.Net.Index.IndexReader;
using TermDocs = Lucene.Net.Index.TermDocs;
using OpenBitSet = Lucene.Net.Util.OpenBitSet;

namespace Lucene.Net.Search
{

    /// <summary> A <see cref="Filter" /> that only accepts documents whose single
    /// term value in the specified field is contained in the
    /// provided set of allowed terms.
    /// 
    /// <p/>
    /// 
    /// This is the same functionality as TermsFilter (from
    /// contrib/queries), except this filter requires that the
    /// field contains only a single term for all documents.
    /// Because of drastically different implementations, they
    /// also have different performance characteristics, as
    /// described below.
    /// 
    /// <p/>
    /// 
    /// The first invocation of this filter on a given field will
    /// be slower, since a <see cref="StringIndex" /> must be
    /// created.  Subsequent invocations using the same field
    /// will re-use this cache.  However, as with all
    /// functionality based on <see cref="FieldCache" />, persistent RAM
    /// is consumed to hold the cache, and is not freed until the
    /// <see cref="IndexReader" /> is closed.  In contrast, TermsFilter
    /// has no persistent RAM consumption.
    /// 
    /// 
    /// <p/>
    /// 
    /// With each search, this filter translates the specified
    /// set of Terms into a private <see cref="OpenBitSet" /> keyed by
    /// term number per unique <see cref="IndexReader" /> (normally one
    /// reader per segment).  Then, during matching, the term
    /// number for each docID is retrieved from the cache and
    /// then checked for inclusion using the <see cref="OpenBitSet" />.
    /// Since all testing is done using RAM resident data
    /// structures, performance should be very fast, most likely
    /// fast enough to not require further caching of the
    /// DocIdSet for each possible combination of terms.
    /// However, because docIDs are simply scanned linearly, an
    /// index with a great many small documents may find this
    /// linear scan too costly.
    /// 
    /// <p/>
    /// 
    /// In contrast, TermsFilter builds up an <see cref="OpenBitSet" />,
    /// keyed by docID, every time it's created, by enumerating
    /// through all matching docs using <see cref="TermDocs" /> to seek
    /// and scan through each term's docID list.  While there is
    /// no linear scan of all docIDs, besides the allocation of
    /// the underlying array in the <see cref="OpenBitSet" />, this
    /// approach requires a number of "disk seeks" in proportion
    /// to the number of terms, which can be exceptionally costly
    /// when there are cache misses in the OS's IO cache.
    /// 
    /// <p/>
    /// 
    /// Generally, this filter will be slower on the first
    /// invocation for a given field, but subsequent invocations,
    /// even if you change the allowed set of Terms, should be
    /// faster than TermsFilter, especially as the number of
    /// Terms being matched increases.  If you are matching only
    /// a very small number of terms, and those terms in turn
    /// match a very small number of documents, TermsFilter may
    /// perform faster.
    /// 
    /// <p/>
    /// 
    /// Which filter is best is very application dependent.
    /// </summary>


        [Serializable]

    public class FieldCacheTermsFilter:Filter
	{
		private readonly string field;
		private readonly string[] terms;
		
		public FieldCacheTermsFilter(string field, params string[] terms)
		{
			this.field = field;
			this.terms = terms;
		}

	    public virtual FieldCache FieldCache
	    {
	        get { return FieldCache_Fields.DEFAULT; }
	    }

	    public override DocIdSet GetDocIdSet(IndexReader reader, IState state)
		{
			return new FieldCacheTermsFilterDocIdSet(this, FieldCache.GetStringIndex(reader, field, state));
		}
		
		protected internal class FieldCacheTermsFilterDocIdSet:DocIdSet
		{
			private void  InitBlock(FieldCacheTermsFilter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private FieldCacheTermsFilter enclosingInstance;
			public FieldCacheTermsFilter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private readonly Lucene.Net.Search.StringIndex fcsi;
			
			private readonly OpenBitSet openBitSet;
			
			public FieldCacheTermsFilterDocIdSet(FieldCacheTermsFilter enclosingInstance, StringIndex fcsi)
			{
				InitBlock(enclosingInstance);
				this.fcsi = fcsi;
				openBitSet = new OpenBitSet(this.fcsi.lookup.Length);
				foreach (string t in Enclosing_Instance.terms)
				{
					int termNumber = this.fcsi.BinarySearchLookup(t);
					if (termNumber > 0)
					{
						openBitSet.FastSet(termNumber);
					}
				}
			}
			
			public override DocIdSetIterator Iterator(IState state)
			{
				return new FieldCacheTermsFilterDocIdSetIterator(this);
			}

		    /// <summary>This DocIdSet implementation is cacheable. </summary>
		    public override bool IsCacheable
		    {
		        get { return true; }
		    }

		    protected internal class FieldCacheTermsFilterDocIdSetIterator:DocIdSetIterator
			{
				public FieldCacheTermsFilterDocIdSetIterator(FieldCacheTermsFilterDocIdSet enclosingInstance)
				{
					InitBlock(enclosingInstance);
				}
				private void  InitBlock(FieldCacheTermsFilterDocIdSet enclosingInstance)
				{
					this.enclosingInstance = enclosingInstance;
				}
				private FieldCacheTermsFilterDocIdSet enclosingInstance;
				public FieldCacheTermsFilterDocIdSet Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				private int doc = - 1;
				
				public override int DocID()
				{
					return doc;
				}
				
				public override int NextDoc(IState state)
				{
					try
					{
						while (!Enclosing_Instance.openBitSet.FastGet(Enclosing_Instance.fcsi.order[++doc]))
						{
						}
					}
					catch (IndexOutOfRangeException)
					{
						doc = NO_MORE_DOCS;
					}
					return doc;
				}
				
				public override int Advance(int target, IState state)
				{
					try
					{
						doc = target;
						while (!Enclosing_Instance.openBitSet.FastGet(Enclosing_Instance.fcsi.order[doc]))
						{
							doc++;
						}
					}
					catch (IndexOutOfRangeException)
					{
						doc = NO_MORE_DOCS;
					}
					return doc;
				}
			}
		}
	}
}