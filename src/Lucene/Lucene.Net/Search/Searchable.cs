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
using Lucene.Net.Documents;
using Lucene.Net.Store;
using Document = Lucene.Net.Documents.Document;
using FieldSelector = Lucene.Net.Documents.FieldSelector;
using CorruptIndexException = Lucene.Net.Index.CorruptIndexException;
using Term = Lucene.Net.Index.Term;

namespace Lucene.Net.Search
{
	
	/// <summary> The interface for search implementations.
	/// 
	/// <p/>
	/// Searchable is the abstract network protocol for searching. Implementations
	/// provide search over a single index, over multiple indices, and over indices
	/// on remote servers.
	/// 
	/// <p/>
	/// Queries, filters and sort criteria are designed to be compact so that they
	/// may be efficiently passed to a remote index, with only the top-scoring hits
	/// being returned, rather than every matching hit.
	/// 
	/// <b>NOTE:</b> this interface is kept public for convenience. Since it is not
	/// expected to be implemented directly, it may be changed unexpectedly between
	/// releases.
	/// </summary>
	public interface Searchable : IDisposable
	{
		/// <summary> Lower-level search API.
		/// 
		/// <p/>
		/// <see cref="Collector.Collect(int)" /> is called for every document. <br/>
		/// Collector-based access to remote indexes is discouraged.
		/// 
		/// <p/>
		/// Applications should only use this if they need <i>all</i> of the matching
		/// documents. The high-level search API (<see cref="Searcher.Search(Query,int)" />) is
		/// usually more efficient, as it skips non-high-scoring hits.
		/// 
		/// </summary>
		/// <param name="weight">to match documents
		/// </param>
		/// <param name="filter">if non-null, used to permit documents to be collected.
		/// </param>
		/// <param name="collector">to receive hits
		/// </param>
		/// <throws>  BooleanQuery.TooManyClauses </throws>
		void  Search(Weight weight, Filter filter, Collector collector, IState state);
		
		/// <summary>Frees resources associated with this Searcher.
		/// Be careful not to call this method while you are still using objects
		/// that reference this searchable
		/// </summary>
		void  Close();
		
		/// <summary>Expert: Returns the number of documents containing <c>term</c>.
		/// Called by search code to compute term weights.
		/// </summary>
		/// <seealso cref="Lucene.Net.Index.IndexReader.DocFreq(Term)">
		/// </seealso>
		int DocFreq(Term term, IState state);
		
		/// <summary>Expert: For each term in the terms array, calculates the number of
		/// documents containing <c>term</c>. Returns an array with these
		/// document frequencies. Used to minimize number of remote calls.
		/// </summary>
		int[] DocFreqs(Term[] terms, IState state);

	    /// <summary>Expert: Returns one greater than the largest possible document number.
	    /// Called by search code to compute term weights.
	    /// </summary>
	    /// <seealso cref="Lucene.Net.Index.IndexReader.MaxDoc">
	    /// </seealso>
	    int MaxDoc { get; }
		
		/// <summary>
		/// Expert: Low-level search implementation.  Finds the top <c>n</c>
		/// hits for <c>query</c>, applying <c>filter</c> if non-null.
		/// 
		/// <p/>Applications should usually call <see cref="Searcher.Search(Query, int)" /> or
		/// <see cref="Searcher.Search(Query,Filter,int)" /> instead.
		/// </summary>
		/// <throws>  BooleanQuery.TooManyClauses </throws>
		TopDocs Search(Weight weight, Filter filter, int n, IState state);
		
		/// <summary>Expert: Returns the stored fields of document <c>i</c>.</summary>
		/// <seealso cref="Lucene.Net.Index.IndexReader.Document(int)" />
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		Document Doc(int i, IState state);

        /// <summary> Get the <see cref="Lucene.Net.Documents.Document" />at the <c>n</c><sup>th</sup> position. The <see cref="Lucene.Net.Documents.FieldSelector"/>
		/// may be used to determine what <see cref="Lucene.Net.Documents.Field" />s to load and how they should be loaded.
		/// 
		/// <b>NOTE:</b> If the underlying Reader (more specifically, the underlying <c>FieldsReader</c>) is closed before the lazy <see cref="Lucene.Net.Documents.Field" /> is
		/// loaded an exception may be thrown.  If you want the value of a lazy <see cref="Lucene.Net.Documents.Field" /> to be available after closing you must
		/// explicitly load it or fetch the Document again with a new loader.
		/// 
		/// 
		/// </summary>
		/// <param name="n">Get the document at the <c>n</c><sup>th</sup> position
		/// </param>
		/// <param name="fieldSelector">The <see cref="Lucene.Net.Documents.FieldSelector" /> to use to determine what Fields should be loaded on the Document.  May be null, in which case all Fields will be loaded.
		/// </param>
		/// <returns> The stored fields of the <see cref="Lucene.Net.Documents.Document" /> at the nth position
		/// </returns>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
		/// <summary> 
		/// </summary>
		/// <seealso cref="Lucene.Net.Index.IndexReader.Document(int, FieldSelector)">
		/// </seealso>
		/// <seealso cref="IFieldable">
		/// </seealso>
		/// <seealso cref="Lucene.Net.Documents.FieldSelector">
		/// </seealso>
		/// <seealso cref="Lucene.Net.Documents.SetBasedFieldSelector">
		/// </seealso>
		/// <seealso cref="Lucene.Net.Documents.LoadFirstFieldSelector">
		/// </seealso>
		Document Doc(int n, FieldSelector fieldSelector, IState state);
		
		/// <summary>Expert: called to re-write queries into primitive queries.</summary>
		/// <throws>  BooleanQuery.TooManyClauses </throws>
		Query Rewrite(Query query, IState state);
		
		/// <summary>Expert: low-level implementation method
		/// Returns an Explanation that describes how <c>doc</c> scored against
		/// <c>weight</c>.
		/// 
		/// <p/>This is intended to be used in developing Similarity implementations,
		/// and, for good performance, should not be displayed with every hit.
		/// Computing an explanation is as expensive as executing the query over the
		/// entire index.
		/// <p/>Applications should call <see cref="Searcher.Explain(Query, int)" />.
		/// </summary>
		/// <throws>  BooleanQuery.TooManyClauses </throws>
		Explanation Explain(Weight weight, int doc, IState state);
		
		/// <summary>Expert: Low-level search implementation with arbitrary sorting.  Finds
		/// the top <c>n</c> hits for <c>query</c>, applying
		/// <c>filter</c> if non-null, and sorting the hits by the criteria in
		/// <c>sort</c>.
		/// 
		/// <p/>Applications should usually call
		/// <see cref="Searcher.Search(Query,Filter,int,Sort)" /> instead.
		/// 
		/// </summary>
		/// <throws>  BooleanQuery.TooManyClauses </throws>
		TopFieldDocs Search(Weight weight, Filter filter, int n, Sort sort, IState state);
	}
}