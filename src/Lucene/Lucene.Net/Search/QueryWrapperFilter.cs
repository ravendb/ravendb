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

namespace Lucene.Net.Search
{

    /// <summary> Constrains search results to only match those which also match a provided
    /// query.  
    /// 
    /// <p/> This could be used, for example, with a <see cref="TermRangeQuery" /> on a suitably
    /// formatted date field to implement date filtering.  One could re-use a single
    /// QueryFilter that matches, e.g., only documents modified within the last
    /// week.  The QueryFilter and TermRangeQuery would only need to be reconstructed
    /// once per day.
    /// 
    /// </summary>
    /// <version>  $Id:$
    /// </version>

        [Serializable]
    public class QueryWrapperFilter:Filter
	{
		private class AnonymousClassDocIdSet:DocIdSet
		{
			public AnonymousClassDocIdSet(Lucene.Net.Search.Weight weight, Lucene.Net.Index.IndexReader reader, QueryWrapperFilter enclosingInstance)
			{
				InitBlock(weight, reader, enclosingInstance);
			}
			private void  InitBlock(Lucene.Net.Search.Weight weight, Lucene.Net.Index.IndexReader reader, QueryWrapperFilter enclosingInstance)
			{
				this.weight = weight;
				this.reader = reader;
				this.enclosingInstance = enclosingInstance;
			}
			private Lucene.Net.Search.Weight weight;
			private Lucene.Net.Index.IndexReader reader;
			private QueryWrapperFilter enclosingInstance;
			public QueryWrapperFilter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override DocIdSetIterator Iterator(IState state)
			{
				return weight.Scorer(reader, true, false, state);
			}

		    public override bool IsCacheable
		    {
		        get { return false; }
		    }
		}
		private Query query;
		
		/// <summary>Constructs a filter which only matches documents matching
		/// <c>query</c>.
		/// </summary>
		public QueryWrapperFilter(Query query)
		{
			this.query = query;
		}
		
		public override DocIdSet GetDocIdSet(IndexReader reader, IState state)
		{
			Weight weight = query.Weight(new IndexSearcher(reader), state);
			return new AnonymousClassDocIdSet(weight, reader, this);
		}
		
		public override System.String ToString()
		{
			return "QueryWrapperFilter(" + query + ")";
		}
		
		public  override bool Equals(System.Object o)
		{
			if (!(o is QueryWrapperFilter))
				return false;
			return this.query.Equals(((QueryWrapperFilter) o).query);
		}
		
		public override int GetHashCode()
		{
			return query.GetHashCode() ^ unchecked((int) 0x923F64B9);
		}
	}
}