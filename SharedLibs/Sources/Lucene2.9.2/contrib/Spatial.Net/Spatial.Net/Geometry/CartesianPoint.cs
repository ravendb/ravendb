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

namespace Lucene.Net.Spatial.Geometry
{
	/// <summary>
	/// Represents lat/lngs as fixed point numbers translated so that all
	/// world coordinates are in the first quadrant.  The same fixed point
	/// scale as is used for FixedLatLng is employed.
	/// </summary>
	public class CartesianPoint
	{
		public CartesianPoint(int x, int y)
		{
			X = x;
			Y = y;
		}

		public int X { get; set; }
		public int Y { get; set; }

		public CartesianPoint Translate(int deltaX, int deltaY)
		{
			return new CartesianPoint(X + deltaX, Y + deltaY);
		}

		public override string ToString()
		{
			return string.Format("Point({0},{1})", X, Y);
		}

		public override int GetHashCode()
		{
			const int prime = 31;
			int result = 1;
			result = prime * result + X;
			result = prime * result + Y;
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj) return true;
			if (obj == null) return false;
			if (GetType() != obj.GetType()) return false;
			
			var other = (CartesianPoint)obj;

			if (X != other.X) return false;
			return Y == other.Y;
		}
	}
}