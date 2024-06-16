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
using Lucene.Net.Index;
using Lucene.Net.Search.Function;
using Lucene.Net.Store;
using Spatial4n.Context;
using Spatial4n.Distance;
using Spatial4n.Shapes;

namespace Lucene.Net.Spatial.Util
{
	/// <summary>
	/// An implementation of the Lucene ValueSource model to support spatial relevance ranking.
	/// </summary>
	public class ShapeFieldCacheDistanceValueSource : ValueSource
	{
		private readonly ShapeFieldCacheProvider<IPoint> provider;
		private readonly SpatialContext ctx;
		private readonly IPoint from;

		public ShapeFieldCacheDistanceValueSource(SpatialContext ctx, ShapeFieldCacheProvider<IPoint> provider, IPoint from)
		{
            this.ctx = ctx;
			this.from = from;
			this.provider = provider;
		}

		public class CachedDistanceDocValues : DocValues
		{
			private readonly ShapeFieldCacheDistanceValueSource enclosingInstance;
			private readonly ShapeFieldCache<IPoint> cache;
		    private readonly IPoint from;
		    private readonly IDistanceCalculator calculator;
		    private readonly double nullValue;

			public CachedDistanceDocValues(IndexReader reader, ShapeFieldCacheDistanceValueSource enclosingInstance, IState state)
			{
                cache = enclosingInstance.provider.GetCache(reader, state);
				this.enclosingInstance = enclosingInstance;
				
                from = enclosingInstance.from;
			    calculator = enclosingInstance.ctx.DistanceCalculator;
			    nullValue = (enclosingInstance.ctx.IsGeo ? 180 : double.MaxValue);
			}

			public override float FloatVal(int doc)
			{
				return (float)DoubleVal(doc);
			}

			public override double DoubleVal(int doc)
			{
				var vals = cache.GetShapes(doc);
				if (vals != null)
				{
                    double v = calculator.Distance(from, vals[0]);
					for (int i = 1; i < vals.Count; i++)
					{
                        v = Math.Min(v, calculator.Distance(from, vals[i]));
					}
					// Solr's 'recip' function where v = distance and v > 0.
					return v > 0 ? 1000 / (1 * v + 1000) : 0;
				}
				return Double.NaN;
			}

			public override string ToString(int doc)
			{
				return enclosingInstance.Description() + "=" + FloatVal(doc);
			}
		}

		public override DocValues GetValues(IndexReader reader, IState state)
		{
			return new CachedDistanceDocValues(reader, this, state);
		}

		public override string Description()
		{
            return GetType().Name + "(" + provider + ", " + from + ")";
		}

		public override bool Equals(object o)
		{
			if (this == o) return true;

			var that = o as ShapeFieldCacheDistanceValueSource;

			if (that == null) return false;
            if (!ctx.Equals(that.ctx)) return false;
            if (!from.Equals(that.from)) return false;
            if (!provider.Equals(that.provider)) return false;

			return true;
		}

		public override int GetHashCode()
		{
		    return from.GetHashCode();
		}
	}
}
