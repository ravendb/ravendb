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

#if !NET35

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Lucene.Net.Store;
using Lucene.Net.Support;
using Lucene.Net.Util;
using IndexReader = Lucene.Net.Index.IndexReader;
using Term = Lucene.Net.Index.Term;

namespace Lucene.Net.Search
{
	/// <summary>Implements parallel search over a set of <c>Searchables</c>.
	/// 
	/// <p/>Applications usually need only call the inherited <see cref="Searcher.Search(Query, int)" />
	/// or <see cref="Searcher.Search(Query,Filter,int)" /> methods.
	/// </summary>
	public class ParallelMultiSearcher : MultiSearcher/*, IDisposable*/ //No need to implement IDisposable like java, nothing to dispose with the TPL
	{
		private class AnonymousClassCollector1:Collector
		{
			public AnonymousClassCollector1(Lucene.Net.Search.Collector collector, int start, ParallelMultiSearcher enclosingInstance)
			{
				InitBlock(collector, start, enclosingInstance);
			}
			private void  InitBlock(Lucene.Net.Search.Collector collector, int start, ParallelMultiSearcher enclosingInstance)
			{
				this.collector = collector;
				this.start = start;
				this.enclosingInstance = enclosingInstance;
			}
			private Lucene.Net.Search.Collector collector;
			private int start;
			private ParallelMultiSearcher enclosingInstance;
			public ParallelMultiSearcher Enclosing_Instance
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
		
		private Searchable[] searchables;
		private int[] starts;
		
		/// <summary>Creates a <see cref="Searchable"/> which searches <i>searchables</i>. </summary>
        public ParallelMultiSearcher(params Searchable[] searchables)
            : base(searchables)
		{
		    this.searchables = searchables;
		    this.starts = GetStarts();
		}

	    /// <summary>
	    /// Executes each <see cref="Searchable"/>'s docFreq() in its own thread and 
	    /// waits for each search to complete and merge the results back together.
	    /// </summary>
		public override int DocFreq(Term term, IState state)
	    {
	        Task<int>[] tasks = new Task<int>[searchables.Length];
            for (int i = 0; i < searchables.Length; i++)
            {
                Searchable searchable = searchables[i];
                tasks[i] = Task.Factory.StartNew(() => searchable.DocFreq(term, state));
            }

	        Task.WaitAll(tasks);
	        return tasks.Sum(task => task.Result);
	    }
		
		/// <summary> A search implementation which executes each
		/// <see cref="Searchable"/> in its own thread and waits for each search to complete
		/// and merge the results back together.
		/// </summary>
		public override TopDocs Search(Weight weight, Filter filter, int nDocs, IState state)
		{
		    HitQueue hq = new HitQueue(nDocs, false);
            object lockObj = new object();

            Task<TopDocs>[] tasks = new Task<TopDocs>[searchables.Length];
            //search each searchable
            for (int i = 0; i < searchables.Length; i++)
            {
                int cur = i;
                tasks[i] =
                    Task.Factory.StartNew(() => MultiSearcherCallableNoSort(ThreadLock.MonitorLock, lockObj, searchables[cur], weight, filter,
                                                                            nDocs, hq, cur, starts, state));
            }

		    int totalHits = 0;
		    float maxScore = float.NegativeInfinity;
            

		    Task.WaitAll(tasks);
            foreach(TopDocs topDocs in tasks.Select(x => x.Result))
            {
                totalHits += topDocs.TotalHits;
                maxScore = Math.Max(maxScore, topDocs.MaxScore);
            }

            ScoreDoc[] scoreDocs = new ScoreDoc[hq.Size()];
            for (int i = hq.Size() - 1; i >= 0; i--) // put docs in array
                scoreDocs[i] = hq.Pop();

		    return new TopDocs(totalHits, scoreDocs, maxScore);
		}
		
		/// <summary> A search implementation allowing sorting which spans a new thread for each
		/// Searchable, waits for each search to complete and merges
		/// the results back together.
		/// </summary>
		public override TopFieldDocs Search(Weight weight, Filter filter, int nDocs, Sort sort, IState state)
		{
            if (sort == null) throw new ArgumentNullException("sort");

		    FieldDocSortedHitQueue hq = new FieldDocSortedHitQueue(nDocs);
            object lockObj = new object();

            Task<TopFieldDocs>[] tasks = new Task<TopFieldDocs>[searchables.Length];
            for (int i = 0; i < searchables.Length; i++) // search each searchable
            {
                int cur = i;
                tasks[i] =
                    Task<TopFieldDocs>.Factory.StartNew(
                        () => MultiSearcherCallableWithSort(ThreadLock.MonitorLock, lockObj, searchables[cur], weight, filter, nDocs, hq, sort, cur,
                                                      starts, state));
            }

		    int totalHits = 0;
		    float maxScore = float.NegativeInfinity;

            Task.WaitAll(tasks);
            foreach (TopFieldDocs topFieldDocs in tasks.Select(x => x.Result))
            {
                totalHits += topFieldDocs.TotalHits;
                maxScore = Math.Max(maxScore, topFieldDocs.MaxScore);
            }

            ScoreDoc[] scoreDocs = new ScoreDoc[hq.Size()];
            for (int i = hq.Size() - 1; i >= 0; i--)
                scoreDocs[i] = hq.Pop();

		    return new TopFieldDocs(totalHits, scoreDocs, hq.GetFields(), maxScore);
		}
		
		/// <summary>Lower-level search API.
		/// 
		/// <p/><see cref="Collector.Collect(int)" /> is called for every matching document.
		/// 
		/// <p/>Applications should only use this if they need <i>all</i> of the
		/// matching documents.  The high-level search API (<see cref="Searcher.Search(Query, int)" />)
		/// is usually more efficient, as it skips
		/// non-high-scoring hits.
		/// <p/>This method cannot be parallelized, because <see cref="Collector"/>
		/// supports no concurrent access.
		/// </summary>
		/// <param name="weight">to match documents
		/// </param>
		/// <param name="filter">if non-null, a bitset used to eliminate some documents
		/// </param>
		/// <param name="collector">to receive hits
		/// 
		/// TODO: parallelize this one too
		/// </param>
		public override void  Search(Weight weight, Filter filter, Collector collector, IState state)
		{
			for (int i = 0; i < searchables.Length; i++)
			{
				
				int start = starts[i];
				
				Collector hc = new AnonymousClassCollector1(collector, start, this);
				
				searchables[i].Search(weight, filter, hc, state);
			}
		}
	}
}

#endif 