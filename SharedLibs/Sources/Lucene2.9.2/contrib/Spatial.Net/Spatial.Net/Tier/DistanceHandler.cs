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

namespace Lucene.Net.Spatial.Tier
{
	public enum Precision
	{
		EXACT,
		TWOFEET,
		TWENTYFEET,
		TWOHUNDREDFEET,
	}

	/// <summary>
	/// Provide a high level access point to distances
	/// Used by DistanceSortSource and DistanceQuery
	/// </summary>
	public class DistanceHandler
	{
		private readonly Dictionary<int, double> _distances;
		private readonly Dictionary<string, double> _distanceLookupCache;
		private readonly Precision? _precision;

		public DistanceHandler(Dictionary<int, double> distances, Dictionary<string, double> distanceLookupCache, Precision precision)
		{
			_distances = distances;
			_distanceLookupCache = distanceLookupCache;
			_precision = precision;
		}

		public static double GetPrecision(double x, Precision? thisPrecision)
		{
			if(thisPrecision.HasValue)
			{
				double dif = 0;

				switch (thisPrecision)
				{
					case Precision.EXACT:
						return x;
					case Precision.TWOFEET:
						dif = x%0.0001;
						break;
					case Precision.TWENTYFEET:
						dif = x%0.001;
						break;
					case Precision.TWOHUNDREDFEET:
						dif = x%0.01;
						break;
				}
				return x - dif;
			}
			return x;
		}

		public Precision GetPrecision()
		{
			return _precision.Value;
		}

		public double GetDistance(int docid, double centerLat, double centerLng, double lat, double lng)
		{
			// check to see if we have distances
			// if not calculate the distance
			if (_distances == null)
			{
				return DistanceUtils.GetInstance().GetDistanceMi(centerLat, centerLng, lat, lng);
			}

			// check to see if the doc id has a cached distance
			double docd;
			_distances.TryGetValue(docid, out docd);
			if (docd > 0) return docd;

			//check to see if we have a precision code
			// and if another lat/long has been calculated at
			// that rounded location
			if (_precision.HasValue)
			{
				double xLat = GetPrecision(lat, _precision);
				double xLng = GetPrecision(lng, _precision);

				String k = xLat + "," + xLng;

				Double d;
				_distanceLookupCache.TryGetValue(k, out d);
				if (d > 0) return d;
			}

			//all else fails calculate the distances    
			return DistanceUtils.GetInstance().GetDistanceMi(centerLat, centerLng, lat, lng);
		}

	}
}