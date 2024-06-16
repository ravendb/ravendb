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
using Lucene.Net.Search;
using Lucene.Net.Search.Function;
using Lucene.Net.Store;

namespace Lucene.Net.Spatial.Util
{
    /// <summary>
    /// Filter that matches all documents where a valuesource is
    /// in between a range of <c>min</c> and <c>max</c> inclusive.
    /// </summary>
	public class ValueSourceFilter : Filter
	{
		readonly Filter startingFilter;
		readonly ValueSource source;
		public readonly double min;
		public readonly double max;

		public ValueSourceFilter(Filter startingFilter, ValueSource source, double min, double max)
		{
			if (startingFilter == null)
			{
				throw new ArgumentException("please provide a non-null startingFilter; you can use QueryWrapperFilter(MatchAllDocsQuery) as a no-op filter", "startingFilter");
			}
			this.startingFilter = startingFilter;
			this.source = source;
			this.min = min;
			this.max = max;
		}

		public override DocIdSet GetDocIdSet(Index.IndexReader reader, IState state)
		{
			var values = source.GetValues(reader, state);
			return new ValueSourceFilteredDocIdSet(startingFilter.GetDocIdSet(reader, state), values, this);
		}

        public class ValueSourceFilteredDocIdSet : FilteredDocIdSet
        {
            private readonly ValueSourceFilter enclosingFilter;
            private readonly DocValues values;

            public ValueSourceFilteredDocIdSet(DocIdSet innerSet, DocValues values, ValueSourceFilter caller)
                : base(innerSet)
            {
                this.enclosingFilter = caller;
                this.values = values;
            }

            public override bool Match(int doc)
            {
                double val = values.DoubleVal(doc);
                return val >= enclosingFilter.min && val <= enclosingFilter.max;
            }
        }
	}
}
