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
using System.Data;
using Lucene.Net.Spatial.Utils;

namespace Lucene.Net.Spatial.Geometry
{
	public class FloatLatLng : LatLng
	{
		private readonly double _lat;
		private readonly double _lng;
		private bool _normalized;

		public FloatLatLng(double lat, double lng)
		{
			if (lat > 90.0 || lat < -90.0)
			{
				throw new ConstraintException("Illegal latitude value " + lat);
			}
			
			_lat = lat;
			_lng = lng;
		}

		public FloatLatLng(LatLng ll)
		{
			_lat = ll.GetLat();
			_lng = ll.GetLng();
		}


		public override bool IsNormalized()
		{
			return _normalized || (_lng >= -180 && _lng <= 180);
		}

		public override bool IsFixedPoint()
		{
			return false;
		}

		public override LatLng Normalize()
		{
			if (IsNormalized()) return this;

			double delta = 0;
			if (_lng < 0) delta = 360;
			if (_lng >= 0) delta = -360;

			double newLng = _lng;
			while (newLng <= -180 || newLng >= 180)
			{
				newLng += delta;
			}

			var ret = new FloatLatLng(_lat, newLng)
			          	{
			          		_normalized = true
			          	};
			return ret;
		}

		public override int GetFixedLat()
		{
			return FixedLatLng.DoubleToFixed(_lat);
		}

		public override int GetFixedLng()
		{
			return FixedLatLng.DoubleToFixed(_lng);
		}

		public override double GetLat()
		{
			return _lat;
		}

		public override double GetLng()
		{
			return _lng;
		}

		public override LatLng Copy()
		{
			return new FloatLatLng(this);
		}

		public override FixedLatLng ToFixed()
		{
			return new FixedLatLng(this);
		}

		public override FloatLatLng ToFloat()
		{
			return this;
		}

		public override LatLng CalculateMidpoint(LatLng other)
		{
			return new FloatLatLng((_lat + other.GetLat()) / 2.0, (_lng + other.GetLng()) / 2.0);
		}

		public override int GetHashCode()
		{
			const int prime = 31;
			long temp = Convert.ToInt64(_lat);
			int result = prime + (int)(temp ^ BitwiseHelper.ZeroFillRightShift(temp, 32));
    
			temp = Convert.ToInt64(_lng);
			result = prime * result + (int)(temp ^ BitwiseHelper.ZeroFillRightShift(temp, 32));
			result = prime * result + (_normalized ? 1231 : 1237);
			
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj) return true;
			if (GetType() != obj.GetType()) return false;
			
			var other = (FloatLatLng)obj;

			if (Convert.ToInt64(_lat) != Convert.ToInt64(other._lat)) return false;
			if (Convert.ToInt64(_lng) != Convert.ToInt64(other._lng)) return false;
			
			return _normalized == other._normalized;
		}
	}
}