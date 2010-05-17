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
using Lucene.Net.Spatial.Tier;

namespace Lucene.Net.Spatial.GeoHash
{
	public class GeoHashDistanceFilter : DistanceFilter
	{

		private readonly double _lat;
		private readonly double _lng;
		private readonly String _geoHashField;

		/// <summary>
		/// Provide a distance filter based from a center point with a radius in miles
		/// </summary>
		/// <param name="startingFilter">The starting filter.</param>
		/// <param name="lat">The lat.</param>
		/// <param name="lng">The LNG.</param>
		/// <param name="miles">The miles.</param>
		/// <param name="geoHashField">The geo hash field.</param>
		public GeoHashDistanceFilter(Filter startingFilter, double lat, double lng, double miles, string geoHashField) : base(startingFilter, miles)
		{
			_lat = lat;
			_lng = lng;
			_geoHashField = geoHashField;
		}

		public override DocIdSet GetDocIdSet(Lucene.Net.Index.IndexReader reader)
		{
			var geoHashValues = FieldCache_Fields.DEFAULT.GetStrings(reader, _geoHashField);
			
			int docBase = NextDocBase;
			NextDocBase += reader.MaxDoc();

			return new GeoHashFilteredDocIdSet(StartingFilter.GetDocIdSet(reader), geoHashValues, DistanceLookupCache, _lat, _lng, docBase, Distance, Distances);
		}

		internal class GeoHashFilteredDocIdSet : FilteredDocIdSet
		{
			public GeoHashFilteredDocIdSet(DocIdSet innerSet, string[] geoHashValues, Dictionary<string, double> distanceLookupCache, double lat, double lng, int docBase, double distance, Dictionary<int, double> distances) : base(innerSet)
			{
				_geoHashValues = geoHashValues;
				_distances = distances;
				_distance = distance;
				_docBase = docBase;
				_lng = lng;
				_lat = lat;
				_distanceLookupCache = distanceLookupCache;
			}

			private readonly double _lat;
			private readonly double _lng;
			private readonly int _docBase;
			private readonly string[] _geoHashValues;
			private readonly Dictionary<int, double> _distances;
			private readonly Dictionary<string, double> _distanceLookupCache;
			private readonly double _distance;

			public override bool Match(int docid)
			{
				String geoHash = _geoHashValues[docid];
				double[] coords = GeoHashUtils.Decode(geoHash);
				double x = coords[0];
				double y = coords[1];

				Double cachedDistance = _distanceLookupCache[geoHash];
				double d;

				if (cachedDistance > 0)
				{
					d = cachedDistance;
				}
				else
				{
					d = DistanceUtils.GetInstance().GetDistanceMi(_lat, _lng, x, y);
					_distanceLookupCache[geoHash] = d;
				}

				if (d < _distance)
				{
					_distances[docid + _docBase] = d;
					return true;
				}
				
				return false;
			}
		}

		public override bool Equals(object o)
		{
			if (this == o) return true;
			if (!(o is GeoHashDistanceFilter)) return false;
   
			var other = (GeoHashDistanceFilter) o;

			if (!StartingFilter.Equals(other.StartingFilter) || Distance != other.Distance || _lat != other._lat || _lng != other._lng || _geoHashField.Equals(other._geoHashField) ) 
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
			h ^= _geoHashField.GetHashCode();

			return h;
		}
	}
}