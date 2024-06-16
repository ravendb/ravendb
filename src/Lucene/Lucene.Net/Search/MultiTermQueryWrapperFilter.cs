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
using Term = Lucene.Net.Index.Term;
using TermDocs = Lucene.Net.Index.TermDocs;
using TermEnum = Lucene.Net.Index.TermEnum;
using OpenBitSet = Lucene.Net.Util.OpenBitSet;

namespace Lucene.Net.Search
{

    /// <summary> A wrapper for <see cref="MultiTermQuery" />, that exposes its
    /// functionality as a <see cref="Filter" />.
    /// <p/>
    /// <c>MultiTermQueryWrapperFilter</c> is not designed to
    /// be used by itself. Normally you subclass it to provide a Filter
    /// counterpart for a <see cref="MultiTermQuery" /> subclass.
    /// <p/>
    /// For example, <see cref="TermRangeFilter" /> and <see cref="PrefixFilter" /> extend
    /// <c>MultiTermQueryWrapperFilter</c>.
    /// This class also provides the functionality behind
    /// <see cref="MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE" />;
    /// this is why it is not abstract.
    /// </summary>

        [Serializable]
    public class MultiTermQueryWrapperFilter<T> : Filter
        where T : MultiTermQuery
	{
        protected internal T query;
		
		/// <summary> Wrap a <see cref="MultiTermQuery" /> as a Filter.</summary>
        protected internal MultiTermQueryWrapperFilter(T query)
		{
			this.query = query;
		}
		
		//@Override
		public override System.String ToString()
		{
			// query.toString should be ok for the filter, too, if the query boost is 1.0f
			return query.ToString();
		}
		
		//@Override
		public  override bool Equals(System.Object o)
		{
			if (o == this)
				return true;
			if (o == null)
				return false;
			if (this.GetType().Equals(o.GetType()))
			{
				return this.query.Equals(((MultiTermQueryWrapperFilter<T>) o).query);
			}
			return false;
		}
		
		//@Override
		public override int GetHashCode()
		{
			return query.GetHashCode();
		}

	    /// <summary> Expert: Return the number of unique terms visited during execution of the filter.
	    /// If there are many of them, you may consider using another filter type
	    /// or optimize your total term count in index.
	    /// <p/>This method is not thread safe, be sure to only call it when no filter is running!
	    /// If you re-use the same filter instance for another
	    /// search, be sure to first reset the term counter
	    /// with <see cref="ClearTotalNumberOfTerms" />.
	    /// </summary>
	    /// <seealso cref="ClearTotalNumberOfTerms">
	    /// </seealso>
	    public virtual int TotalNumberOfTerms
	    {
	        get { return query.TotalNumberOfTerms; }
	    }

	    /// <summary> Expert: Resets the counting of unique terms.
		/// Do this before executing the filter.
		/// </summary>
		/// <seealso cref="TotalNumberOfTerms">
		/// </seealso>
		public virtual void  ClearTotalNumberOfTerms()
		{
			query.ClearTotalNumberOfTerms();
		}

        public override DocIdSet GetDocIdSet(IndexReader reader, IState state)
        {
            TermEnum enumerator = query.GetEnum(reader, state);
            try
            {
                // if current term in enum is null, the enum is empty -> shortcut
                if (enumerator.Term == null)
                    return DocIdSet.EMPTY_DOCIDSET;
                // else fill into an OpenBitSet
                OpenBitSet bitSet = new OpenBitSet(reader.MaxDoc);
                Span<int> docs = stackalloc int[32];
                Span<int> freqs = stackalloc int[32];

                TermDocs termDocs = reader.TermDocs(state);
                try
                {
                    int termCount = 0;
                    do
                    {
                        Term term = enumerator.Term;
                        if (term == null)
                            break;
                        termCount++;
                        termDocs.Seek(term, state);
                        while (true)
                        {
                            int count = termDocs.Read(docs, freqs, state);
                            if (count != 0)
                            {
                                for (int i = 0; i < count; i++)
                                {
                                    bitSet.Set(docs[i]);
                                }
                            }
                            else
                            {
                                break;
                            }
                        }
                    } while (enumerator.Next(state));

                    query.IncTotalNumberOfTerms(termCount); // {{Aroush-2.9}} is the use of 'temp' as is right?
                }
                finally
                {
                    termDocs.Close();
                }

				return bitSet;
			}
			finally
			{
				enumerator.Close();
			}
		}
	}
}