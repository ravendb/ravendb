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

using System.Data;

namespace Lucene.Net.Spatial.Geometry
{
	public class FixedLatLng : LatLng
	{
		public const double ScaleFactor = 1000000;
		public const int ScaleFactorInt = 1000000;

		private int _lat;
		private int _lng;
		private bool _normalized;

		public FixedLatLng(int lat, int lng)
		{
			SetLat(lat);
			SetLng(lng);
		}

		public FixedLatLng(LatLng ll)
		{
			_lat = ll.GetFixedLat();
			_lng = ll.GetFixedLng();
		}

		private void SetLat(int lat)
		{
			if (lat > 90 * ScaleFactor || lat < -90 * ScaleFactor)
			{
				throw new ConstraintException("Illegal lattitude");
			}

			_lat = lat;
		}

		protected void SetLng(int lng)
		{
			_lng = lng;
		}

		public static double FixedToDouble(int fixedInt) 
		{
			return fixedInt / ScaleFactor;
		}
		  
		public static int DoubleToFixed(double d) 
		{
			return (int)(d*ScaleFactor);
		}

		public override bool IsNormalized()
		{
			return _normalized || (_lng >= -180 * ScaleFactorInt && _lng <= 180 * ScaleFactorInt);
		}

		public override bool IsFixedPoint()
		{
			return true;
		}

		public override LatLng Normalize()
		{
			if (IsNormalized()) return this;
    
			int delta = 0;
			if (_lng < 0) delta = 360 * ScaleFactorInt;
			if (_lng >= 0) delta =- 360 * ScaleFactorInt;
		    
			int newLng = _lng;
			while (newLng <= -180 * ScaleFactorInt || newLng >= 180 * ScaleFactorInt)
			{
				newLng += delta;
			}

			var ret = new FixedLatLng(_lat, newLng)
			          	{
			          		_normalized = true
			          	};

			return ret;

		}

		public override int GetFixedLat()
		{
			return _lat;
		}

		public override int GetFixedLng()
		{
			return _lng;
		}

		public override double GetLat()
		{
			return FixedToDouble(_lat);
		}

		public override double GetLng()
		{
			return FixedToDouble(_lng);
		}

		public override LatLng Copy()
		{
			return new FixedLatLng(this);
		}

		public override FixedLatLng ToFixed()
		{
			return this;
		}

		public override FloatLatLng ToFloat()
		{
			return new FloatLatLng(this);
		}

		public override LatLng CalculateMidpoint(LatLng other)
		{
			return new FixedLatLng((_lat + other.GetFixedLat())/2, (_lng + other.GetFixedLng())/2);
		}

		public override int GetHashCode()
		{
			const int prime = 31;
			int result = prime + _lat;
			result = prime * result + _lng;
			result = prime * result + (_normalized ? 1231 : 1237);
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj) return true;
			if (GetType() != obj.GetType()) return false;
			
			var other = (FixedLatLng)obj;
			
			if (_lat != other._lat) return false;
			if (_lng != other._lng) return false;
			if (_normalized != other._normalized) return false;
			return true;
		}
	}
}