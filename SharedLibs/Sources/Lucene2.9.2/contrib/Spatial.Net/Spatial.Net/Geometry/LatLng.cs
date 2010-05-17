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
using Lucene.Net.Spatial.Utils;

namespace Lucene.Net.Spatial.Geometry
{
	public abstract class LatLng
	{
		public abstract bool IsNormalized();
		public abstract bool IsFixedPoint();
		public abstract LatLng Normalize();
		public abstract int GetFixedLat();
		public abstract int GetFixedLng();
		public abstract double GetLat();
		public abstract double GetLng();
		public abstract LatLng Copy();
		public abstract FixedLatLng ToFixed();
		public abstract FloatLatLng ToFloat();

		/// <summary>
		/// Convert the lat/lng into the cartesian coordinate plane such that all
		/// world coordinates are represented in the first quadrant.
		/// The x dimension corresponds to latitude and y corresponds to longitude.
		/// The translation starts with the normalized latlng and adds 180 to the latitude and 
		/// 90 to the longitude (subject to fixed point scaling).
		/// </summary>
		public CartesianPoint ToCartesian()
		{
			LatLng ll = Normalize();

			int lat = ll.GetFixedLat();
			int lng = ll.GetFixedLng();

			return new CartesianPoint(
				lng + 180 * FixedLatLng.ScaleFactorInt,
				lat + 90 * FixedLatLng.ScaleFactorInt
				);
		}

		/// <summary>
		///The inverse of ToCartesian().  Always returns a FixedLatLng.
		/// </summary>
		public static LatLng FromCartesian(CartesianPoint pt)
		{
			int lat = pt.Y - 90 * FixedLatLng.ScaleFactorInt;
			int lng = pt.X - 180 * FixedLatLng.ScaleFactorInt;

			return new FixedLatLng(lat, lng);
		}

		/// <summary>
		/// Calculates the distance between two lat/lng's in miles.
		/// </summary>
		/// <param name="latLng">The lat lng.</param>
		/// <returns>Returns the distance in miles</returns>
		public double ArcDistance(LatLng ll2)
		{
			return ArcDistance(ll2, DistanceUnits.MILES);
		}

		/// <summary>
		///Calculates the distance between two lat/lng's in miles or meters.
		/// </summary>
		/// <param name="ll2">Second lat,lng position to calculate distance to.</param>
		/// <param name="lUnits">Units to calculate distance, defaults to miles</param>
		/// <returns>Returns the distance in meters or miles</returns>
		public double ArcDistance(LatLng ll2, DistanceUnits lUnits)
		{
			LatLng ll1 = Normalize();
			ll2 = ll2.Normalize();

			double lat1 = ll1.GetLat(), lng1 = ll1.GetLng();
			double lat2 = ll2.GetLat(), lng2 = ll2.GetLng();

			// Check for same position
			if (lat1 == lat2 && lng1 == lng2)
				return 0.0;

			// Get the m_dLongitude difference. Don't need to worry about
			// crossing 180 since cos(x) = cos(-x)
			double dLon = lng2 - lng1;

			double a = Radians(90.0 - lat1);
			double c = Radians(90.0 - lat2);
			double cosB = (Math.Cos(a) * Math.Cos(c))
			              + (Math.Sin(a) * Math.Sin(c) * Math.Cos(Radians(dLon)));

			double radius = (lUnits == DistanceUnits.MILES) ? 3963.205 /* MILERADIUSOFEARTH */ : 6378.160187
				/* KMRADIUSOFEARTH */;

			// Find angle subtended (with some bounds checking) in radians and
			// multiply by earth radius to find the arc distance
			if (cosB < -1.0) return MathHelper.PI * radius;

			if (cosB >= 1.0) return 0;

			return Math.Acos(cosB) * radius;
		}

		private static double Radians(double a)
		{
			return a * 0.01745329251994;
		}

		public override string ToString()
		{
			return string.Format("[{0},{1}]", GetLat(), GetLng());
		}

		/// <summary>
		/// Calculate the midpoint between this point an another.  Respects fixed vs floating point
		/// </summary>
		/// <param name="other">The other.</param>
		/// <returns></returns>
		public abstract LatLng CalculateMidpoint(LatLng other);

		public abstract int GetHashCode();

		public abstract bool Equals(Object obj);
	}
}