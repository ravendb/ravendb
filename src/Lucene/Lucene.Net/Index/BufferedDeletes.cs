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
using System.Runtime.CompilerServices;
using Lucene.Net.Search;
using Lucene.Net.Support;
using Lucene.Net.Util;

namespace Lucene.Net.Index
{
    // Number of documents a delete term applies to.
    internal sealed class DeleteTermNum
    {
        internal int num;

        internal DeleteTermNum(int num)
        {
            this.num = num;
        }

        internal int GetNum()
        {
            return num;
        }

        internal void SetNum(int num)
        {
            // Only record the new number if it's greater than the
            // current one.  This is important because if multiple
            // threads are replacing the same doc at nearly the
            // same time, it's possible that one thread that got a
            // higher docID is scheduled before the other
            // threads.
            if (num > this.num)
                this.num = num;
        }
    }

    /// <summary>Holds buffered deletes, by docID, term or query.  We
    /// hold two instances of this class: one for the deletes
    /// prior to the last flush, the other for deletes after
    /// the last flush.  This is so if we need to abort
    /// (discard all buffered docs) we can also discard the
    /// buffered deletes yet keep the deletes done during
    /// previously flushed segments. 
    /// </summary>
    class SortedBufferedDeletes
    {
        internal struct DeleteTerm : IComparable<DeleteTerm>
        {
            internal Term Term;
            internal DeleteTermNum Number;

            public DeleteTerm(Term term, DeleteTermNum number)
            {
                this.Term = term;
                this.Number = number;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int CompareTo(DeleteTerm other)
            {
                return Term.CompareTo(other.Term);
            }
        }

        internal struct DeleteComparer : IComparer<DeleteTerm>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(DeleteTerm x, DeleteTerm y)
            {
                return x.Term.CompareTo(y.Term);
            }
        }

        internal int numTerms;
        
        internal FastList<DeleteTerm> terms;
        internal HashMap<Query, int> queries = new HashMap<Query, int>();
        internal List<int> docIDs = new List<int>();
        internal long bytesUsed;
        internal const bool doTermSort = true;
        private static Sorter<DeleteTerm, DeleteComparer> sorter = default(Sorter<DeleteTerm, DeleteComparer>);

        public SortedBufferedDeletes()
        {
            terms = new FastList<DeleteTerm>();
        }

        internal virtual int Size()
        {
            // We use numTerms not terms.size() intentionally, so
            // that deletes by the same term multiple times "count",
            // ie if you ask to flush every 1000 deletes then even
            // dup'd terms are counted towards that 1000
            return numTerms + queries.Count + docIDs.Count;
        }

        internal void Update(SortedBufferedDeletes @in)
        {
            numTerms += @in.numTerms;
            bytesUsed += @in.bytesUsed;

            terms.AddRange(@in.terms);
            terms.Sort(ref sorter);

            foreach (var term in @in.queries)
            {
                queries[term.Key] = term.Value;
            }

            docIDs.AddRange(@in.docIDs);
            @in.Clear();
        }

        internal void Update(UnsortedBufferedDeletes @in)
        {
            numTerms += @in.numTerms;
            bytesUsed += @in.bytesUsed;

            if (numTerms != 0)
            {
                // Grow the list if necessary. 
                if( terms.Capacity < numTerms)
                    terms.Capacity = numTerms;

                foreach (var term in @in.terms)
                    terms.Add(new DeleteTerm(term.Key, term.Value));

                terms.Sort(ref sorter);
            }

            foreach (var term in @in.queries)
            {
                queries[term.Key] = term.Value;
            }

            docIDs.AddRange(@in.docIDs);
            @in.Clear();
        }

        internal void Clear()
        {
            terms.Clear();
            queries.Clear();
            docIDs.Clear();
            numTerms = 0;
            bytesUsed = 0;
        }

        internal void AddBytesUsed(long b)
        {
            bytesUsed += b;
        }

        internal bool Any()
        {
            return terms.Count > 0 || docIDs.Count > 0 || queries.Count > 0;
        }

        // Remaps all buffered deletes based on a completed
        // merge
        internal void Remap(MergeDocIDRemapper mapper, SegmentInfos infos, int[][] docMaps, int[] delCounts, MergePolicy.OneMerge merge, int mergeDocCount)
        {
            lock (this)
            {
                FastList<DeleteTerm> newDeleteTerms;

                // Remap delete-by-term
                if (terms.Count > 0)
                {
                    newDeleteTerms = new FastList<DeleteTerm>(terms.Count);
                    foreach (var entry in terms)
                    {
                        DeleteTermNum num = entry.Number;
                        // Adding because it is already sorted.
                        newDeleteTerms.Add(new DeleteTerm(entry.Term, new DeleteTermNum(mapper.Remap(num.GetNum()))));
                    }
                }
                else
                    newDeleteTerms = null;

                // Remap delete-by-docID
                List<int> newDeleteDocIDs;

                if (docIDs.Count > 0)
                {
                    newDeleteDocIDs = new List<int>(docIDs.Count);
                    foreach (int num in docIDs)
                    {
                        newDeleteDocIDs.Add(mapper.Remap(num));
                    }
                }
                else
                    newDeleteDocIDs = null;

                // Remap delete-by-query
                HashMap<Query, int> newDeleteQueries;

                if (queries.Count > 0)
                {
                    newDeleteQueries = new HashMap<Query, int>(queries.Count);
                    foreach (var entry in queries)
                    {
                        int num = entry.Value;
                        newDeleteQueries[entry.Key] = mapper.Remap(num);
                    }
                }
                else
                    newDeleteQueries = null;

                if (newDeleteTerms != null)
                    terms = newDeleteTerms;
                if (newDeleteDocIDs != null)
                    docIDs = newDeleteDocIDs;
                if (newDeleteQueries != null)
                    queries = newDeleteQueries;
            }
        }
    }

    /// <summary>Holds buffered deletes, by docID, term or query.  We
    /// hold two instances of this class: one for the deletes
    /// prior to the last flush, the other for deletes after
    /// the last flush.  This is so if we need to abort
    /// (discard all buffered docs) we can also discard the
    /// buffered deletes yet keep the deletes done during
    /// previously flushed segments. 
    /// </summary>
    class UnsortedBufferedDeletes
    {
        internal int numTerms;
        internal HashMap<Term, DeleteTermNum> terms = null;
        internal HashMap<Query, int> queries = new HashMap<Query, int>();
        internal List<int> docIDs = new List<int>();
        internal long bytesUsed;
        internal const bool doTermSort = false;

        public UnsortedBufferedDeletes()
        {
            terms = new HashMap<Term, DeleteTermNum>();
        }

        internal int Size()
        {
            // We use numTerms not terms.size() intentionally, so
            // that deletes by the same term multiple times "count",
            // ie if you ask to flush every 1000 deletes then even
            // dup'd terms are counted towards that 1000
            return numTerms + queries.Count + docIDs.Count;
        }

        internal void Update(UnsortedBufferedDeletes @in)
        {
            numTerms += @in.numTerms;
            bytesUsed += @in.bytesUsed;
            foreach (var term in @in.terms)
            {
                terms[term.Key] = term.Value;
            }
            foreach (var term in @in.queries)
            {
                queries[term.Key] = term.Value;
            }

            docIDs.AddRange(@in.docIDs);
            @in.Clear();
        }

        internal void Clear()
        {
            terms.Clear();
            queries.Clear();
            docIDs.Clear();
            numTerms = 0;
            bytesUsed = 0;
        }

        internal void AddBytesUsed(long b)
        {
            bytesUsed += b;
        }

        internal bool Any()
        {
            return terms.Count > 0 || docIDs.Count > 0 || queries.Count > 0;
        }

        // Remaps all buffered deletes based on a completed
        // merge
        internal virtual void Remap(MergeDocIDRemapper mapper, SegmentInfos infos, int[][] docMaps, int[] delCounts, MergePolicy.OneMerge merge, int mergeDocCount)
        {
            lock (this)
            {
                HashMap<Term, DeleteTermNum> newDeleteTerms;

                // Remap delete-by-term
                if (terms.Count > 0)
                {
                    newDeleteTerms = new HashMap<Term, DeleteTermNum>();
                    foreach (var entry in terms)
                    {
                        DeleteTermNum num = entry.Value;
                        newDeleteTerms[entry.Key] = new DeleteTermNum(mapper.Remap(num.GetNum()));
                    }
                }
                else
                    newDeleteTerms = null;

                // Remap delete-by-docID
                List<int> newDeleteDocIDs;

                if (docIDs.Count > 0)
                {
                    newDeleteDocIDs = new List<int>(docIDs.Count);
                    foreach (int num in docIDs)
                    {
                        newDeleteDocIDs.Add(mapper.Remap(num));
                    }
                }
                else
                    newDeleteDocIDs = null;

                // Remap delete-by-query
                HashMap<Query, int> newDeleteQueries;

                if (queries.Count > 0)
                {
                    newDeleteQueries = new HashMap<Query, int>(queries.Count);
                    foreach (var entry in queries)
                    {
                        int num = entry.Value;
                        newDeleteQueries[entry.Key] = mapper.Remap(num);
                    }
                }
                else
                    newDeleteQueries = null;

                if (newDeleteTerms != null)
                    terms = newDeleteTerms;
                if (newDeleteDocIDs != null)
                    docIDs = newDeleteDocIDs;
                if (newDeleteQueries != null)
                    queries = newDeleteQueries;
            }
        }
    }


}