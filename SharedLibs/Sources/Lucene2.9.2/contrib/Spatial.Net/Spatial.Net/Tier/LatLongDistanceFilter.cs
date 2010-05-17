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

using System.Collections.Generic;
using Lucene.Net.Index;
using Lucene.Net.Search;

namespace Lucene.Net.Spatial.Tier
{
	public class LatLongDistanceFilter : DistanceFilter
	{
		private readonly double _lat;
		private readonly double _lng;
		private readonly string _latField;
		private readonly string _lngField;

		public LatLongDistanceFilter(Filter startingFilter, double distance, double lat, double lng, string latField, string lngField) : base(startingFilter, distance)
		{
			_lat = lat;
			_lngField = lngField;
			_latField = latField;
			_lng = lng;
		}

		public override DocIdSet GetDocIdSet(IndexReader reader)
		{
			double[] latIndex = FieldCache_Fields.DEFAULT.GetDoubles(reader, _latField);
			double[] lngIndex = FieldCache_Fields.DEFAULT.GetDoubles(reader, _lngField);

			int docBase = NextDocBase;
			NextDocBase += reader.MaxDoc();

			return new LatLongFilteredDocIdSet(StartingFilter.GetDocIdSet(reader), latIndex, lngIndex, DistanceLookupCache, _lat, _lng, Distance, docBase, Distances);
		}

		internal class LatLongFilteredDocIdSet : FilteredDocIdSet
		{
			private readonly double _lat;
			private readonly double _lng;
			private readonly int _docBase;
			private readonly double _distance;
			private readonly double[] _latIndex;
			private readonly double[] _lngIndex;
			private readonly Dictionary<string, double> _distanceLookupCache;
			private readonly Dictionary<int, double> _distances;

			public LatLongFilteredDocIdSet(DocIdSet innerSet, double[] latIndex, double[] lngIndex, Dictionary<string, double> distanceLookupCache, double lat, double lng, double distance, int docBase, Dictionary<int, double> distances)
				: base(innerSet)
			{
				_latIndex = latIndex;
				_distances = distances;
				_docBase = docBase;
				_distance = distance;
				_lng = lng;
				_lat = lat;
				_distanceLookupCache = distanceLookupCache;
				_lngIndex = lngIndex;
			}

			public override bool Match(int docid)
			{
				double x = _latIndex[docid];
				double y = _lngIndex[docid];

				string ck = x + "," + y;
				double cachedDistance = _distanceLookupCache.ContainsKey(ck) ? _distanceLookupCache[ck] : 0;

				double d;
				if (cachedDistance > 0)
				{
					d = cachedDistance;
				}
				else
				{
					d = DistanceUtils.GetInstance().GetDistanceMi(_lat, _lng, x, y);
					_distanceLookupCache[ck] = d;
				}

				if (d < _distance)
				{
					// Save distances, so they can be pulled for
					// sorting after filtering is done:
					_distances[docid + _docBase] = d;
					return true;
				}

				return false;
			}

		}

		public override bool Equals(object o)
		{
			if (this == o) return true;
			if (!(o is LatLongDistanceFilter)) return false;

			var other = (LatLongDistanceFilter) o;

			if (!StartingFilter.Equals(other.StartingFilter) || Distance != other.Distance || _lat != other._lat ||
			    _lng != other._lng || !_latField.Equals(other._latField) || !_lngField.Equals(other._lngField))
			{
				return false;
			}

			return true;
		}

		public override int GetHashCode()
		{
			int h = Distance.GetHashCode();
			h ^= StartingFilter.GetHashCode();
			h ^= _lat.GetHashCode();
			h ^= _lng.GetHashCode();
			h ^= _latField.GetHashCode();
			h ^= _lngField.GetHashCode();

			return h;
		}
	}
}