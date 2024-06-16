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
using System.Linq;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Document = Lucene.Net.Documents.Document;
using FieldSelector = Lucene.Net.Documents.FieldSelector;
using CorruptIndexException = Lucene.Net.Index.CorruptIndexException;
using IndexReader = Lucene.Net.Index.IndexReader;
using Term = Lucene.Net.Index.Term;
using Directory = Lucene.Net.Store.Directory;
using ReaderUtil = Lucene.Net.Util.ReaderUtil;

namespace Lucene.Net.Search
{

    /// <summary>Implements search over a single IndexReader.
    /// 
    /// <p/>Applications usually need only call the inherited <see cref="Searcher.Search(Query,int)" />
    /// or <see cref="Searcher.Search(Query,Filter,int)" /> methods. For performance reasons it is 
    /// recommended to open only one IndexSearcher and use it for all of your searches.
    /// 
    /// <a name="thread-safety"></a><p/><b>NOTE</b>:
    /// <see cref="IndexSearcher" /> instances are completely
    /// thread safe, meaning multiple threads can call any of its
    /// methods, concurrently.  If your application requires
    /// external synchronization, you should <b>not</b>
    /// synchronize on the <c>IndexSearcher</c> instance;
    /// use your own (non-Lucene) objects instead.<p/>
    /// </summary>

        [Serializable]
    public class IndexSearcher : Searcher
	{
		internal IndexReader reader;
		private bool closeReader;
	    private bool isDisposed;

        // NOTE: these members might change in incompatible ways
        // in the next release
		private IndexReader[] subReaders;
		private int[] docStarts;
		
		/// <summary>Creates a searcher searching the index in the named
		/// directory, with readOnly=true</summary>
		/// <throws>CorruptIndexException if the index is corrupt</throws>
		/// <throws>IOException if there is a low-level IO error</throws>
        public IndexSearcher(Directory path, IState state)
            : this(IndexReader.Open(path, true, state), true)
		{
		}
		
		/// <summary>Creates a searcher searching the index in the named
		/// directory.  You should pass readOnly=true, since it
		/// gives much better concurrent performance, unless you
		/// intend to do write operations (delete documents or
		/// change norms) with the underlying IndexReader.
		/// </summary>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		/// <param name="path">directory where IndexReader will be opened
		/// </param>
		/// <param name="readOnly">if true, the underlying IndexReader
		/// will be opened readOnly
		/// </param>
		public IndexSearcher(Directory path, bool readOnly, IState state) :this(IndexReader.Open(path, readOnly, state), true)
		{
		}

        /// <summary>Creates a searcher searching the provided index
        /// <para>
        /// Note that the underlying IndexReader is not closed, if
        /// IndexSearcher was constructed with IndexSearcher(IndexReader r).
        /// If the IndexReader was supplied implicitly by specifying a directory, then
        /// the IndexReader gets closed.
        /// </para>
        /// </summary>
		public IndexSearcher(IndexReader r):this(r, false)
		{
		}
		
        /// <summary>
        /// Expert: directly specify the reader, subReaders and their
        /// DocID starts
        /// <p/>
        /// <b>NOTE:</b> This API is experimental and
        /// might change in incompatible ways in the next
        /// release<p/>
        /// </summary>
        public IndexSearcher(IndexReader reader, IndexReader[] subReaders, int[] docStarts)
        {
            this.reader = reader;
            this.subReaders = subReaders;
            this.docStarts = docStarts;
            this.closeReader = false;
        }

		private IndexSearcher(IndexReader r, bool closeReader)
		{
			reader = r;
			this.closeReader = closeReader;

		    System.Collections.Generic.IList<IndexReader> subReadersList = new System.Collections.Generic.List<IndexReader>();
			GatherSubReaders(subReadersList, reader);
            subReaders = subReadersList.ToArray();
			docStarts = new int[subReaders.Length];
			int maxDoc = 0;
			for (int i = 0; i < subReaders.Length; i++)
			{
				docStarts[i] = maxDoc;
				maxDoc += subReaders[i].MaxDoc;
			}
		}
		
		protected internal virtual void  GatherSubReaders(System.Collections.Generic.IList<IndexReader> allSubReaders, IndexReader r)
		{
			ReaderUtil.GatherSubReaders(allSubReaders, r);
		}

	    /// <summary>Return the <see cref="Index.IndexReader" /> this searches. </summary>
	    public virtual IndexReader IndexReader
	    {
	        get { return reader; }
	    }

	    protected override void Dispose(bool disposing)
        {
            if (isDisposed) return;

            if (disposing)
            {
                if (closeReader)
                    reader.Close();
            }

            isDisposed = true;
        }
		
		// inherit javadoc
		public override int DocFreq(Term term, IState state)
		{
			return reader.DocFreq(term, state);
		}
		
		// inherit javadoc
		public override Document Doc(int i, IState state)
		{
			return reader.Document(i, state);
		}
		
		// inherit javadoc
		public override Document Doc(int i, FieldSelector fieldSelector, IState state)
		{
			return reader.Document(i, fieldSelector, state);
		}
		
		// inherit javadoc
		public override int MaxDoc
		{
            get { return reader.MaxDoc; }
		}
		
		// inherit javadoc
		public override TopDocs Search(Weight weight, Filter filter, int nDocs, IState state)
		{
			
			if (nDocs <= 0)
			{
				throw new System.ArgumentException("nDocs must be > 0");
			}
            nDocs = Math.Min(nDocs, reader.MaxDoc);

			TopScoreDocCollector collector = TopScoreDocCollector.Create(nDocs, !weight.GetScoresDocsOutOfOrder());
			Search(weight, filter, collector, state);
			return collector.TopDocs();
		}
		
		public override TopFieldDocs Search(Weight weight, Filter filter, int nDocs, Sort sort, IState state)
		{
			return Search(weight, filter, nDocs, sort, true, state);
		}
		
		/// <summary> Just like <see cref="Search(Weight, Filter, int, Sort)" />, but you choose
		/// whether or not the fields in the returned <see cref="FieldDoc" /> instances
		/// should be set by specifying fillFields.
		/// <p/>
		/// NOTE: this does not compute scores by default. If you need scores, create
		/// a <see cref="TopFieldCollector" /> instance by calling
		/// <see cref="TopFieldCollector.Create" /> and then pass that to
		/// <see cref="Search(Weight, Filter, Collector)" />.
		/// <p/>
		/// </summary>
		public virtual TopFieldDocs Search(Weight weight, Filter filter, int nDocs, Sort sort, bool fillFields, IState state)
		{
            nDocs = Math.Min(nDocs, reader.MaxDoc);

			TopFieldCollector collector2 = TopFieldCollector.Create(sort, nDocs, fillFields, fieldSortDoTrackScores, fieldSortDoMaxScore, !weight.GetScoresDocsOutOfOrder());
			Search(weight, filter, collector2, state);
			return (TopFieldDocs) collector2.TopDocs();
		}
		
		public override void  Search(Weight weight, Filter filter, Collector collector, IState state)
		{
			
			if (filter == null)
			{
				for (int i = 0; i < subReaders.Length; i++)
				{
					// search each subreader
					collector.SetNextReader(subReaders[i], docStarts[i], state);
					Scorer scorer = weight.Scorer(subReaders[i], !collector.AcceptsDocsOutOfOrder, true, state);
					if (scorer != null)
					{
						scorer.Score(collector, state);
					}
				}
			}
			else
			{
				for (int i = 0; i < subReaders.Length; i++)
				{
					// search each subreader
					collector.SetNextReader(subReaders[i], docStarts[i], state);
					SearchWithFilter(subReaders[i], weight, filter, collector, state);
				}
			}
		}
		
		private void  SearchWithFilter(IndexReader reader, Weight weight, Filter filter, Collector collector, IState state)
		{
			
			System.Diagnostics.Debug.Assert(filter != null);
			
			Scorer scorer = weight.Scorer(reader, true, false, state);
			if (scorer == null)
			{
				return ;
			}
			
			int docID = scorer.DocID();
			System.Diagnostics.Debug.Assert(docID == - 1 || docID == DocIdSetIterator.NO_MORE_DOCS);
			
			// CHECKME: use ConjunctionScorer here?
			DocIdSet filterDocIdSet = filter.GetDocIdSet(reader, state);
			if (filterDocIdSet == null)
			{
				// this means the filter does not accept any documents.
				return ;
			}
			
			DocIdSetIterator filterIter = filterDocIdSet.Iterator(state);
			if (filterIter == null)
			{
				// this means the filter does not accept any documents.
				return ;
			}
			int filterDoc = filterIter.NextDoc(state);
			int scorerDoc = scorer.Advance(filterDoc, state);
			
			collector.SetScorer(scorer);
			while (true)
			{
				if (scorerDoc == filterDoc)
				{
					// Check if scorer has exhausted, only before collecting.
					if (scorerDoc == DocIdSetIterator.NO_MORE_DOCS)
					{
						break;
					}
					collector.Collect(scorerDoc, state);
					filterDoc = filterIter.NextDoc(state);
					scorerDoc = scorer.Advance(filterDoc, state);
				}
				else if (scorerDoc > filterDoc)
				{
					filterDoc = filterIter.Advance(scorerDoc, state);
				}
				else
				{
					scorerDoc = scorer.Advance(filterDoc, state);
				}
			}
		}
		
		public override Query Rewrite(Query original, IState state)
		{
			Query query = original;
			for (Query rewrittenQuery = query.Rewrite(reader, state); rewrittenQuery != query; rewrittenQuery = query.Rewrite(reader, state))
			{
				query = rewrittenQuery;
			}
			return query;
		}
		
		public override Explanation Explain(Weight weight, int doc, IState state)
		{
			int n = ReaderUtil.SubIndex(doc, docStarts);
			int deBasedDoc = doc - docStarts[n];
			
			return weight.Explain(subReaders[n], deBasedDoc, state);
		}
		
		private bool fieldSortDoTrackScores;
		private bool fieldSortDoMaxScore;
		
		/// <summary> By default, no scores are computed when sorting by field (using
		/// <see cref="Searcher.Search(Query,Filter,int,Sort)" />). You can change that, per
		/// IndexSearcher instance, by calling this method. Note that this will incur
		/// a CPU cost.
		/// 
		/// </summary>
		/// <param name="doTrackScores">If true, then scores are returned for every matching document
		/// in <see cref="TopFieldDocs" />.
		/// 
		/// </param>
		/// <param name="doMaxScore">If true, then the max score for all matching docs is computed.
		/// </param>
		public virtual void  SetDefaultFieldSortScoring(bool doTrackScores, bool doMaxScore)
		{
			fieldSortDoTrackScores = doTrackScores;
			fieldSortDoMaxScore = doMaxScore;
		}

        public IndexReader reader_ForNUnit
        {
            get { return reader; }
        }
	}
}