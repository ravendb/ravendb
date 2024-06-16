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
using CorruptIndexException = Lucene.Net.Index.CorruptIndexException;
using IndexReader = Lucene.Net.Index.IndexReader;
using Term = Lucene.Net.Index.Term;
using ReaderUtil = Lucene.Net.Util.ReaderUtil;

namespace Lucene.Net.Search
{
	
	/// <summary>Implements search over a set of <c>Searchables</c>.
	/// 
	/// <p/>Applications usually need only call the inherited <see cref="Searcher.Search(Query, int)" />
	/// or <see cref="Searcher.Search(Query,Filter, int)" /> methods.
	/// </summary>
	public class MultiSearcher:Searcher
	{
		private class AnonymousClassCollector:Collector
		{
			public AnonymousClassCollector(Lucene.Net.Search.Collector collector, int start, MultiSearcher enclosingInstance)
			{
				InitBlock(collector, start, enclosingInstance);
			}
			private void  InitBlock(Lucene.Net.Search.Collector collector, int start, MultiSearcher enclosingInstance)
			{
				this.collector = collector;
				this.start = start;
				this.enclosingInstance = enclosingInstance;
			}
			private Lucene.Net.Search.Collector collector;
			private int start;
			private MultiSearcher enclosingInstance;
			public MultiSearcher Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override void  SetScorer(Scorer scorer)
			{
				collector.SetScorer(scorer);
			}
			public override void  Collect(int doc, IState state)
			{
				collector.Collect(doc, state);
			}
			public override void  SetNextReader(IndexReader reader, int docBase, IState state)
			{
				collector.SetNextReader(reader, start + docBase, state);
			}

		    public override bool AcceptsDocsOutOfOrder
		    {
		        get { return collector.AcceptsDocsOutOfOrder; }
		    }
		}
		
		/// <summary> Document Frequency cache acting as a Dummy-Searcher. This class is no
		/// full-fledged Searcher, but only supports the methods necessary to
		/// initialize Weights.
		/// </summary>
		private class CachedDfSource:Searcher
		{
			private readonly Dictionary<Term,int> dfMap; // Map from Terms to corresponding doc freqs
			private readonly int maxDoc; // document count
			
			public CachedDfSource(Dictionary<Term,int> dfMap, int maxDoc, Similarity similarity)
			{
				this.dfMap = dfMap;
				this.maxDoc = maxDoc;
				Similarity = similarity;
			}
			
			public override int DocFreq(Term term, IState state)
			{
				int df;
				try
				{
					df = dfMap[term];
				}
				catch (KeyNotFoundException) // C# equiv. of java code.
				{
					throw new System.ArgumentException("df for term " + term.Text + " not available");
				}
				return df;
			}
			
			public override int[] DocFreqs(Term[] terms, IState state)
			{
				int[] result = new int[terms.Length];
				for (int i = 0; i < terms.Length; i++)
				{
					result[i] = DocFreq(terms[i], state);
				}
				return result;
			}
			
			public override int MaxDoc
			{
                get { return maxDoc; }
			}
			
			public override Query Rewrite(Query query, IState state)
			{
				// this is a bit of a hack. We know that a query which
				// creates a Weight based on this Dummy-Searcher is
				// always already rewritten (see preparedWeight()).
				// Therefore we just return the unmodified query here
				return query;
			}

            // TODO: This probably shouldn't throw an exception?
            protected override void Dispose(bool disposing)
            {
                throw new System.NotSupportedException();
            }
			
			public override Document Doc(int i, IState state)
			{
				throw new System.NotSupportedException();
			}
			
			public override Document Doc(int i, FieldSelector fieldSelector, IState state)
			{
				throw new System.NotSupportedException();
			}
			
			public override Explanation Explain(Weight weight, int doc, IState state)
			{
				throw new System.NotSupportedException();
			}
			
			public override void  Search(Weight weight, Filter filter, Collector results, IState state)
			{
				throw new System.NotSupportedException();
			}
			
			public override TopDocs Search(Weight weight, Filter filter, int n, IState state)
			{
				throw new System.NotSupportedException();
			}
			
			public override TopFieldDocs Search(Weight weight, Filter filter, int n, Sort sort, IState state)
			{
				throw new System.NotSupportedException();
			}
		}
		
		private Searchable[] searchables;
		private int[] starts;
		private int maxDoc = 0;

	    private bool isDisposed;
		
		/// <summary>Creates a searcher which searches <i>searchers</i>. </summary>
		public MultiSearcher(params Searchable[] searchables)
		{
			this.searchables = searchables;
			
			starts = new int[searchables.Length + 1]; // build starts array
			for (int i = 0; i < searchables.Length; i++)
			{
				starts[i] = maxDoc;
				maxDoc += searchables[i].MaxDoc; // compute maxDocs
			}
			starts[searchables.Length] = maxDoc;
		}
		
		/// <summary>Return the array of <see cref="Searchable" />s this searches. </summary>
		public virtual Searchable[] GetSearchables()
		{
			return searchables;
		}
		
		protected internal virtual int[] GetStarts()
		{
			return starts;
		}

        protected override void Dispose(bool disposing)
        {
            if (isDisposed) return;

            if (disposing)
            {
                for (int i = 0; i < searchables.Length; i++)
                    searchables[i].Close();
            }

            isDisposed = true;
        }

		public override int DocFreq(Term term, IState state)
		{
			int docFreq = 0;
			for (int i = 0; i < searchables.Length; i++)
				docFreq += searchables[i].DocFreq(term, state);
			return docFreq;
		}
		
		// inherit javadoc
		public override Document Doc(int n, IState state)
		{
			int i = SubSearcher(n); // find searcher index
			return searchables[i].Doc(n - starts[i], state); // dispatch to searcher
		}
		
		// inherit javadoc
		public override Document Doc(int n, FieldSelector fieldSelector, IState state)
		{
			int i = SubSearcher(n); // find searcher index
			return searchables[i].Doc(n - starts[i], fieldSelector, state); // dispatch to searcher
		}
		
		/// <summary>Returns index of the searcher for document <c>n</c> in the array
		/// used to construct this searcher. 
		/// </summary>
		public virtual int SubSearcher(int n)
		{
			// find searcher for doc n:
			return ReaderUtil.SubIndex(n, starts);
		}
		
		/// <summary>Returns the document number of document <c>n</c> within its
		/// sub-index. 
		/// </summary>
		public virtual int SubDoc(int n)
		{
			return n - starts[SubSearcher(n)];
		}

	    public override int MaxDoc
	    {
	        get { return maxDoc; }
	    }

	    public override TopDocs Search(Weight weight, Filter filter, int nDocs, IState state)
		{
			HitQueue hq = new HitQueue(nDocs, false);
			int totalHits = 0;

            var lockObj = new object();
			for (int i = 0; i < searchables.Length; i++)
			{
                // search each searcher
                // use NullLock, we don't care about synchronization for these
                TopDocs docs = MultiSearcherCallableNoSort(ThreadLock.NullLock, lockObj, searchables[i], weight, filter, nDocs, hq, i, starts, state);
				totalHits += docs.TotalHits; // update totalHits
			}
			
			ScoreDoc[] scoreDocs2 = new ScoreDoc[hq.Size()];
			for (int i = hq.Size() - 1; i >= 0; i--)
			// put docs in array
				scoreDocs2[i] = hq.Pop();
			
			float maxScore = (totalHits == 0)?System.Single.NegativeInfinity:scoreDocs2[0].Score;
			
			return new TopDocs(totalHits, scoreDocs2, maxScore);
		}
		
		public override TopFieldDocs Search(Weight weight, Filter filter, int n, Sort sort, IState state)
		{
			var hq = new FieldDocSortedHitQueue(n);
			int totalHits = 0;
			
			float maxScore = System.Single.NegativeInfinity;

		    var lockObj = new object();
			for (int i = 0; i < searchables.Length; i++)
			{
				// search each searcher
                // use NullLock, we don't care about synchronization for these
                TopFieldDocs docs = MultiSearcherCallableWithSort(ThreadLock.NullLock, lockObj, searchables[i], weight, filter, n, hq, sort,
			                                          i, starts, state);
			    totalHits += docs.TotalHits;
				maxScore = System.Math.Max(maxScore, docs.MaxScore);
			}
			
			ScoreDoc[] scoreDocs2 = new ScoreDoc[hq.Size()];
			for (int i = hq.Size() - 1; i >= 0; i--)
			// put docs in array
				scoreDocs2[i] = hq.Pop();
			
			return new TopFieldDocs(totalHits, scoreDocs2, hq.GetFields(), maxScore);
		}
		
		///<inheritdoc />
		public override void  Search(Weight weight, Filter filter, Collector collector, IState state)
		{
			for (int i = 0; i < searchables.Length; i++)
			{
				int start = starts[i];
				
				Collector hc = new AnonymousClassCollector(collector, start, this);
				searchables[i].Search(weight, filter, hc, state);
			}
		}
		
		public override Query Rewrite(Query original, IState state)
		{
			Query[] queries = new Query[searchables.Length];
			for (int i = 0; i < searchables.Length; i++)
			{
				queries[i] = searchables[i].Rewrite(original, state);
			}
			return queries[0].Combine(queries);
		}
		
		public override Explanation Explain(Weight weight, int doc, IState state)
		{
			int i = SubSearcher(doc); // find searcher index
			return searchables[i].Explain(weight, doc - starts[i], state); // dispatch to searcher
		}
		
		/// <summary> Create weight in multiple index scenario.
		/// 
		/// Distributed query processing is done in the following steps:
		/// 1. rewrite query
		/// 2. extract necessary terms
		/// 3. collect dfs for these terms from the Searchables
		/// 4. create query weight using aggregate dfs.
		/// 5. distribute that weight to Searchables
		/// 6. merge results
		/// 
		/// Steps 1-4 are done here, 5+6 in the search() methods
		/// 
		/// </summary>
		/// <returns> rewritten queries
		/// </returns>
		public /*protected internal*/ override Weight CreateWeight(Query original, IState state)
		{
			// step 1
			Query rewrittenQuery = Rewrite(original, state);
			
			// step 2
		    ISet<Term> terms = Lucene.Net.Support.Compatibility.SetFactory.CreateHashSet<Term>();
			rewrittenQuery.ExtractTerms(terms);
			
			// step3
		    Term[] allTermsArray = terms.ToArray();
            int[] aggregatedDfs = new int[terms.Count];
			for (int i = 0; i < searchables.Length; i++)
			{
				int[] dfs = searchables[i].DocFreqs(allTermsArray, state);
				for (int j = 0; j < aggregatedDfs.Length; j++)
				{
					aggregatedDfs[j] += dfs[j];
				}
			}
			
			var dfMap = new Dictionary<Term, int>();
			for (int i = 0; i < allTermsArray.Length; i++)
			{
				dfMap[allTermsArray[i]] = aggregatedDfs[i];
			}
			
			// step4
			int numDocs = MaxDoc;
			CachedDfSource cacheSim = new CachedDfSource(dfMap, numDocs, Similarity);
			
			return rewrittenQuery.Weight(cacheSim, state);
		}

	    internal Func<ThreadLock, object, Searchable, Weight, Filter, int, HitQueue, int, int[], IState, TopDocs> MultiSearcherCallableNoSort =
	        (threadLock, lockObj, searchable, weight, filter, nDocs, hq, i, starts, state) =>
	            {
	                TopDocs docs = searchable.Search(weight, filter, nDocs, state);
	                ScoreDoc[] scoreDocs = docs.ScoreDocs;
                    for(int j = 0; j < scoreDocs.Length; j++) // merge scoreDocs into hq
                    {
                        ScoreDoc scoreDoc = scoreDocs[j];
                        scoreDoc.Doc += starts[i]; //convert doc
                        //it would be so nice if we had a thread-safe insert
                        try
                        {
                            threadLock.Enter(lockObj);
                            if (scoreDoc == hq.InsertWithOverflow(scoreDoc))
                                break;
                        }
                        finally
                        {
                            threadLock.Exit(lockObj);
                        }
                    }
	                return docs;
	            };

        internal Func<ThreadLock, object, Searchable, Weight, Filter, int, FieldDocSortedHitQueue, Sort, int, int[], IState, TopFieldDocs>
            MultiSearcherCallableWithSort = (threadLock, lockObj, searchable, weight, filter, nDocs, hq, sort, i, starts, state) =>
	                                            {
	                                                TopFieldDocs docs = searchable.Search(weight, filter, nDocs, sort, state);
                                                    // if one of the Sort fields is FIELD_DOC, need to fix its values, so that
                                                    // it will break ties by doc Id properly.  Otherwise, it will compare to
                                                    // 'relative' doc Ids, that belong to two different searchables.
                                                    for (int j = 0; j < docs.fields.Count; j++)
                                                    {
                                                        if (docs.fields.Array[j + docs.fields.Offset].Type == SortField.DOC)
                                                        {
                                                            // iterate over the score docs and change their fields value
                                                            for (int j2 = 0; j2 < docs.ScoreDocs.Length; j2++)
                                                            {
                                                                FieldDoc fd = (FieldDoc) docs.ScoreDocs[j2];
                                                                fd.fields[j] = (int)fd.fields[j] + starts[i];
                                                            }
                                                            break;
                                                        }
                                                    }
	                                                try
                                                    {
                                                        threadLock.Enter(lockObj);
                                                        hq.SetFields(docs.fields);
	                                                }
	                                                finally
                                                    {
                                                        threadLock.Exit(lockObj);
	                                                }

	                                                ScoreDoc[] scoreDocs = docs.ScoreDocs;
                                                    for (int j = 0; j < scoreDocs.Length; j++) // merge scoreDocs into hq
                                                    {
                                                        FieldDoc fieldDoc = (FieldDoc) scoreDocs[j];
                                                        fieldDoc.Doc += starts[i]; //convert doc
                                                        //it would be so nice if we had a thread-safe insert
                                                        lock (lockObj)
                                                        {
                                                            if (fieldDoc == hq.InsertWithOverflow(fieldDoc))
                                                                break;

                                                        }
                                                    }
	                                                return docs;
	                                            };
	}
}