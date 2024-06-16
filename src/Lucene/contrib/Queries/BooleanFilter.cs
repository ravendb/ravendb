/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
using System.Text;

using Lucene.Net.Index;
using Lucene.Net.Support;
using Lucene.Net.Util;

namespace Lucene.Net.Search
{
    public class BooleanFilter : Filter
    {
        /// <summary>
        /// The filters that are optional clauses.
        /// </summary>
        private List<Filter> shouldFilters = null;

        /// <summary>
        /// The filters that are used for exclusion.
        /// </summary>
        private List<Filter> notFilters = null;

        /// <summary>
        /// The filters that must be met.
        /// </summary>
        private List<Filter> mustFilters = null;

        /// <summary>
        /// Get the iterator for a specific filter.
        /// </summary>
        /// <param name="filters">The list of filters</param>
        /// <param name="index">The index of the iterator to get.</param>
        /// <param name="reader">The reader for the index.</param>
        /// <returns></returns>
        private DocIdSetIterator GetDISI(List<Filter> filters, int index, IndexReader reader)
        {
            return filters[index].GetDocIdSet(reader).Iterator();
        }

        /// <summary>
        /// Get the id set for the filter.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns>The filter set to use.</returns>
        public override DocIdSet GetDocIdSet(IndexReader reader)
        {
            OpenBitSetDISI res = null;

            if (shouldFilters != null)
            {
                for (int i = 0; i < shouldFilters.Count; i++)
                {
                    if (res == null)
                    {
                        res = new OpenBitSetDISI(GetDISI(shouldFilters, i, reader), reader.MaxDoc);
                    }
                    else
                    {
                        DocIdSet dis = shouldFilters[i].GetDocIdSet(reader);
                        if (dis is OpenBitSet)
                        {
                            // optimized case for OpenBitSets
                            res.Or((OpenBitSet)dis);
                        }
                        else
                        {
                            res.InPlaceOr(GetDISI(shouldFilters, i, reader));
                        }
                    }
                }
            }

            if (notFilters != null)
            {
                for (int i = 0; i < notFilters.Count; i++)
                {
                    if (res == null)
                    {
                        res = new OpenBitSetDISI(GetDISI(notFilters, i, reader), reader.MaxDoc);
                        res.Flip(0, reader.MaxDoc); // NOTE: may set bits on deleted docs
                    }
                    else
                    {
                        DocIdSet dis = notFilters[i].GetDocIdSet(reader);
                        if (dis is OpenBitSet)
                        {
                            // optimized case for OpenBitSets
                            res.AndNot((OpenBitSet)dis);
                        }
                        else
                        {
                            res.InPlaceNot(GetDISI(notFilters, i, reader));
                        }
                    }
                }
            }

            if (mustFilters != null)
            {
                for (int i = 0; i < mustFilters.Count; i++)
                {
                    if (res == null)
                    {
                        res = new OpenBitSetDISI(GetDISI(mustFilters, i, reader), reader.MaxDoc);
                    }
                    else
                    {
                        DocIdSet dis = mustFilters[i].GetDocIdSet(reader);
                        if (dis is OpenBitSet)
                        {
                            // optimized case for OpenBitSets
                            res.And((OpenBitSet)dis);
                        }
                        else
                        {
                            res.InPlaceAnd(GetDISI(mustFilters, i, reader));
                        }
                    }
                }
            }

            if (res != null)
                return FinalResult(res, reader.MaxDoc);

            return DocIdSet.EMPTY_DOCIDSET;
        }

        /* Provide a SortedVIntList when it is definitely smaller
         * than an OpenBitSet.
         * @deprecated Either use CachingWrapperFilter, or
         * switch to a different DocIdSet implementation yourself. 
         * This method will be removed in Lucene 4.0
         */
        protected DocIdSet FinalResult(OpenBitSetDISI result, int maxDocs)
        {
            return result;
        }

        /// <summary>
        /// Add a filter clause.
        /// </summary>
        /// <param name="filterClause">The clause to add.</param>
        public void Add(FilterClause filterClause)
        {
            if (filterClause.Occur == Occur.MUST)
            {
                if (mustFilters == null)
                {
                    mustFilters = new EquatableList<Filter>();
                }
                mustFilters.Add(filterClause.Filter);
            }
            if (filterClause.Occur == Occur.SHOULD)
            {
                if (shouldFilters == null)
                {
                    shouldFilters = new EquatableList<Filter>();
                }
                shouldFilters.Add(filterClause.Filter);
            }
            if (filterClause.Occur == Occur.MUST_NOT)
            {
                if (notFilters == null)
                {
                    notFilters = new EquatableList<Filter>();
                }
                notFilters.Add(filterClause.Filter);
            }
        }

        /// <summary>
        /// Determine equality between two lists.
        /// </summary>
        /// <param name="filters1"></param>
        /// <param name="filters2"></param>
        /// <returns></returns>
        private bool EqualFilters(List<Filter> filters1, List<Filter> filters2)
        {
            return (filters1 == filters2) ||
                     ((filters1 != null) && filters1.Equals(filters2));
        }

        /// <summary>
        /// Equality
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(Object obj)
        {
            if (this == obj)
                return true;

            if ((obj == null) || !(obj is BooleanFilter))
                return false;

            BooleanFilter other = (BooleanFilter)obj;
            return EqualFilters(notFilters, other.notFilters)
                && EqualFilters(mustFilters, other.mustFilters)
                && EqualFilters(shouldFilters, other.shouldFilters);
        }

        /// <summary>
        /// Hash code.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            int hash = 7;
            hash = 31 * (hash + this.ListHash(this.mustFilters)); 
            hash = 31 * (hash + this.ListHash(this.notFilters)); 
            hash = 31 * (hash + this.ListHash(this.shouldFilters));
            return hash;
        }


        private int ListHash(List<Filter> filters)
        {
            int sum = 0;
            if (filters != null && filters.Count > 0)
            {
                for (int i = 0; i < filters.Count; i++)
                {
                    sum += filters[i].GetHashCode();
                }
            }
            return sum;
        }

        /// <summary>
        /// String representation.
        /// </summary>
        /// <returns></returns>
        public override String ToString()
        {
            StringBuilder buffer = new StringBuilder();
            buffer.Append("BooleanFilter(");
            AppendFilters(shouldFilters, "", buffer);
            AppendFilters(mustFilters, "+", buffer);
            AppendFilters(notFilters, "-", buffer);
            buffer.Append(")");
            return buffer.ToString();
        }

        /// <summary>
        /// Append individual filters.
        /// </summary>
        /// <param name="filters"></param>
        /// <param name="occurString"></param>
        /// <param name="buffer"></param>
        private void AppendFilters(List<Filter> filters, String occurString, StringBuilder buffer)
        {
            if (filters != null)
            {
                for (int i = 0; i < filters.Count(); i++)
                {
                    buffer.Append(' ');
                    buffer.Append(occurString);
                    buffer.Append(filters[i].ToString());
                }
            }
        }
    }
}
