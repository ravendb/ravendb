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
using Lucene.Net.Search;

namespace Lucene.Net.Spatial.Tier
{
	public abstract class DistanceFilter : Filter
	{
		protected Filter StartingFilter;
		protected Precision Precision;
		protected double Distance;

		protected int NextDocBase;
		protected Dictionary<string, double> DistanceLookupCache;

		protected DistanceFilter(Filter startingFilter, double distance)
		{
			if (startingFilter == null)
			{
				throw new ArgumentNullException("startingFilter", "Please provide a non-null startingFilter; you can use QueryWrapperFilter(MatchAllDocsQuery) as a no-op filter");
			}

			StartingFilter = startingFilter;
			Distance = distance;

			// NOTE: neither of the distance filters use precision
			// now - if we turn that on, we'll need to pass top
			// reader into here
			// setPrecision(reader.maxDoc());

			/* store calculated distances for reuse by other components */
			Distances = new Dictionary<int, Double>();

			// create an intermediate cache to avoid recomputing distances for the same point 
			DistanceLookupCache = new Dictionary<string, double>();
		}

		public Dictionary<int, double> Distances { get; set; }

		public double GetDistance(int docid)
		{
			return Distances.ContainsKey(docid) ? Distances[docid]: 0;
		}

		/// <summary>
		/// You must call this before re-using this DistanceFilter across searches
		/// </summary>
		public void Reset()
		{
			NextDocBase = 0;
		}

		public abstract bool Equals(Object o);

		public abstract int GetHashCode();
	}
}