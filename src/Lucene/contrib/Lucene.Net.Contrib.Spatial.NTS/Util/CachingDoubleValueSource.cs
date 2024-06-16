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

using System.Collections.Generic;
using Lucene.Net.Index;
using Lucene.Net.Search.Function;
using Lucene.Net.Store;

namespace Lucene.Net.Spatial.Util
{
	public class CachingDoubleValueSource : ValueSource
	{
		protected readonly ValueSource source;
		protected readonly Dictionary<int, double> cache;

		public CachingDoubleValueSource(ValueSource source)
		{
			this.source = source;
			cache = new Dictionary<int, double>();
		}

		public class CachingDoubleDocValue : DocValues
		{
			private readonly int docBase;
			private readonly DocValues values;
			private readonly Dictionary<int, double> cache;

			public CachingDoubleDocValue(int docBase, DocValues vals, Dictionary<int, double> cache)
			{
				this.docBase = docBase;
				this.values = vals;
				this.cache = cache;
			}

			public override double DoubleVal(int doc)
			{
				var key = docBase + doc;
				double v;
				if (!cache.TryGetValue(key, out v))
				{
					v = values.DoubleVal(doc);
					cache[key] = v;
				}
				return v;
			}

			public override float FloatVal(int doc)
			{
				return (float)DoubleVal(doc);
			}

			public override string ToString(int doc)
			{
				return DoubleVal(doc) + string.Empty;
			}
		}

		public override DocValues GetValues(IndexReader reader, IState state)
		{
			var @base = 0; //reader.DocBase;
			var vals = source.GetValues(reader, state);
			return new CachingDoubleDocValue(@base, vals, cache);

		}

		public override string Description()
		{
			return "Cached[" + source.Description() + "]";
		}

		public override bool Equals(object o)
		{
			if (this == o) return true;

			var that = o as CachingDoubleValueSource;

			if (that == null) return false;
			if (source != null ? !source.Equals(that.source) : that.source != null) return false;

			return true;
		}

		public override int GetHashCode()
		{
			return source != null ? source.GetHashCode() : 0;
		}
	}
}
