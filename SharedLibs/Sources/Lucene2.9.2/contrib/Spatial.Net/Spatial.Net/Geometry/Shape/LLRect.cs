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

namespace Lucene.Net.Spatial.Geometry.Shape
{
	/// <summary>
	/// Lat-long rect.  Instances are mutable.
	/// </summary>
	public class LLRect
	{
		private LatLng _ll;
		private LatLng _ur;

		public LLRect(LatLng ll, LatLng ur)
		{
			_ll = ll;
			_ur = ur;
		}

		public LLRect(LLRect other)
		{
			_ll = other._ll;
			_ur = other._ur;
		}

		/// <summary>
		/// Return the area in units of lat-lng squared.  This is a contrived unit
		/// that only has value when comparing to something else.
		/// </summary>
		public double Area()
		{
			return Math.Abs((_ll.GetLat() - _ur.GetLat()) * (_ll.GetLng() - _ur.GetLng()));
		}

		public LatLng GetLowerLeft()
		{
			return _ll;
		}

		public LatLng GetUpperRight()
		{
			return _ur;
		}

		public LatLng GetMidpoint()
		{
			return _ll.CalculateMidpoint(_ur);
		}

		/// <summary>
		/// Approximates a box centered at the given point with the given width and height in miles.
		/// </summary>
		/// <param name="center">The center.</param>
		/// <param name="widthMi">The width mi.</param>
		/// <param name="heightMi">The height mi.</param>
		/// <returns></returns>
		public static LLRect CreateBox(LatLng center, double widthMi, double heightMi)
		{
			double d = widthMi;
			LatLng ur = BoxCorners(center, d, 45.0); // assume right angles
			LatLng ll = BoxCorners(center, d, 225.0);

			return new LLRect(ll, ur);
		}

		private static LatLng BoxCorners(LatLng center, double d, double brngdeg)
		{
			double a = center.GetLat();
			double b = center.GetLng();
			double R = 3963.0; // radius of earth in miles
			double brng = (MathHelper.PI * brngdeg / 180);
			double lat1 = (MathHelper.PI * a / 180);
			double lon1 = (MathHelper.PI * b / 180);

			// Haversine formula
			double lat2 = Math.Asin(Math.Sin(lat1) * Math.Cos(d / R) +
			                        Math.Cos(lat1) * Math.Sin(d / R) * Math.Cos(brng));
			double lon2 = lon1 + Math.Atan2(Math.Sin(brng) * Math.Sin(d / R) * Math.Cos(lat1),
			                                Math.Cos(d / R) - Math.Sin(lat1) * Math.Sin(lat2));

			lat2 = (lat2 * 180) / MathHelper.PI;
			lon2 = (lon2 * 180) / MathHelper.PI;

			// normalize long first
			LatLng ll = NormLng(lat2, lon2);

			// normalize lat - could flip poles
			ll = NormLat(ll.GetLat(), ll.GetLng());

			return ll;
		}

		/// <summary>
		/// Returns a normalized Lng rectangle shape for the bounding box
		/// </summary>
		private static LatLng NormLng(double lat, double lng)
		{
			if (lng > 180.0)
			{
				lng = -1.0 * (180.0 - (lng - 180.0));
			}
			else if (lng < -180.0)
			{
				lng = (lng + 180.0) + 180.0;
			}
			LatLng ll = new FloatLatLng(lat, lng);
			return ll;
		}

		/// <summary>
		/// Returns a normalized Lat rectangle shape for the bounding box
		/// If you go over the poles, you need to flip the lng value too
		/// </summary>
		private static LatLng NormLat(double lat, double lng)
		{
			if (lat > 90.0)
			{
				lat = 90.0 - (lat - 90.0);
				if (lng < 0)
				{
					lng = lng + 180;
				}
				else
				{
					lng = lng - 180;
				}
			}
			else if (lat < -90.0)
			{
				lat = -90.0 - (lat + 90.0);
				if (lng < 0)
				{
					lng = lng + 180;
				}
				else
				{
					lng = lng - 180;
				}
			}
			LatLng ll = new FloatLatLng(lat, lng);
			return ll;
		}

		public Rectangle ToRectangle()
		{
			return new Rectangle(_ll.GetLng(), _ll.GetLat(), _ur.GetLng(), _ur.GetLat());
		}

		public override string ToString()
		{
			return "{" + _ll +", " + _ur +"}";
		}

		public override int GetHashCode()
		{
			const int prime = 31;
			int result = 1;
			result = prime * result + ((_ll == null) ? 0 : _ll.GetHashCode());
			result = prime * result + ((_ur == null) ? 0 : _ur.GetHashCode());
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj) return true;
			if (obj == null) return false;
			if (GetType() != obj.GetType()) return false;
			
			var other = (LLRect)obj;

			if (_ll == null)
			{
				if (other._ll != null) return false;
			}
			else if (!_ll.Equals(other._ll)) return false;
			if (_ur == null)
			{
				if (other._ur != null) return false;
			}
			else if (!_ur.Equals(other._ur)) return false;
			
			return true;
		}
	}
}