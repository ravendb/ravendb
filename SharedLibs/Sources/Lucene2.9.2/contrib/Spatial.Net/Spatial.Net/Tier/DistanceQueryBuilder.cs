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
using Lucene.Net.Search;
using Lucene.Net.Spatial.GeoHash;

namespace Lucene.Net.Spatial.Tier
{
	public class DistanceQueryBuilder
	{
		/// <summary>
		/// Create a distance query using
		/// a boundary box wrapper around a more precise
		/// DistanceFilter.
		/// </summary>
		/// <param name="lat">The lat.</param>
		/// <param name="lng">The Lng.</param>
		/// <param name="miles">The miles.</param>
		/// <param name="latField">The lat field.</param>
		/// <param name="lngField">The Lng field.</param>
		/// <param name="tierFieldPrefix">The tier field prefix.</param>
		/// <param name="needPrecise">if set to <c>true</c> [need precise].</param>
		public DistanceQueryBuilder(double lat, double lng, double miles, string latField, string lngField, string tierFieldPrefix, bool needPrecise)
		{
			Lat = lat;
			Lng = lng;
			Miles = miles;

			var cpf = new CartesianPolyFilterBuilder(tierFieldPrefix);
			Filter cartesianFilter = cpf.GetBoundingArea(lat, lng, miles);

			/* create precise distance filter */
			if (needPrecise)
			{
				_filter = DistanceFilter = new LatLongDistanceFilter(cartesianFilter, miles, lat, lng, latField, lngField);
			}
			else
			{
				_filter = cartesianFilter;
				DistanceFilter = null;
			}
		}

		/// <summary>
		/// Create a distance query using
		/// a boundary box wrapper around a more precise
		/// DistanceFilter.
		/// </summary>
		/// <param name="lat">The lat.</param>
		/// <param name="lng">The Lng.</param>
		/// <param name="miles">The miles.</param>
		/// <param name="geoHashFieldPrefix">The geo hash field prefix.</param>
		/// <param name="tierFieldPrefix">The tier field prefix.</param>
		/// <param name="needPrecise">if set to <c>true</c> [need precise].</param>
		public DistanceQueryBuilder(double lat, double lng, double miles, string geoHashFieldPrefix, String tierFieldPrefix, bool needPrecise)
		{

			Lat = lat;
			Lng = lng;
			Miles = miles;

			var cpf = new CartesianPolyFilterBuilder(tierFieldPrefix);
			Filter cartesianFilter = cpf.GetBoundingArea(lat, lng, miles);

			/* create precise distance filter */
			if (needPrecise)
			{
				_filter = DistanceFilter = new GeoHashDistanceFilter(cartesianFilter, lat, lng, miles, geoHashFieldPrefix);
			}
			else
			{
				_filter = cartesianFilter;
				DistanceFilter = null;
			}
		}


		public double Lat { get; private set; }
		public double Lng { get; private set; }
		public double Miles { get; private set; }
		public DistanceFilter DistanceFilter { get; private set; }

		private readonly Filter _filter;
		public Filter Filter
		{
			get
			{
				if (DistanceFilter != null)
				{
					DistanceFilter.Reset();
				}

				return _filter;
			}
		}

		public Filter GetFilter(Query query)
		{
			throw new NotImplementedException("Relies on ChainedFilter (contrib/misc). Do we need this?"); //{{rogchap}}
			// Chain the Query (as filter) with our distance filter
			/*if (_distanceFilter != null)
			{
				_distanceFilter.Reset();
			}
			var qf = new QueryWrapperFilter(query);
			return new ChainedFilter(new [] { qf, _filter }, ChainedFilter.AND);
			 */
		}

		public Query GetQuery(Query query)
		{
			return new ConstantScoreQuery(GetFilter(query));
		}

		public override string ToString()
		{
			return "DistanceQuery lat: " + Lat + " lng: " + Lng + " miles: " + Miles;
		}
	}
}