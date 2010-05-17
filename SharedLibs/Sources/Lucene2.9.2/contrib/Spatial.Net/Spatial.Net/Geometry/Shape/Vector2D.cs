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
	/// 2D Vector
	/// </summary>
	public class Vector2D
	{
		public Vector2D()
		{
			X = 0;
			Y = 0;
		}

		/// <summary>
		/// Create a vector from the origin of the coordinate system to the given point
		/// </summary>
		/// <param name="x">The x.</param>
		/// <param name="y">The y.</param>
		public Vector2D(double x, double y)
		{
			X = x;
			Y = y;
		}

		/// <summary>
		/// Create a vector from the origin of the coordinate system to the given point
		/// </summary>
		public Vector2D(Point2D p) : this(p.X, p.Y) {}

		/// <summary>
		/// Create a vector from one point to another
		/// </summary>
		public Vector2D(Point2D from, Point2D to) : this(to.X - from.X, to.Y - from.Y) {}

		public Vector2D(Vector2D other)
		{
			X = other.X;
			Y = other.Y;
		}

		public double X { get; set; }
		public double Y { get; set; }

		public void Set(double x, double y)
		{
			X = x;
			Y = y;
		}

		public bool Equals(Vector2D other)
		{
			return other != null && X == other.X && Y == other.Y;
		}

		public double Dot(Vector2D v) 
		{
			return ((X) * v.X) + (Y * v.Y);
		}

		/// <summary>
		/// Vector length (magnitude) squared
		/// </summary>
		public double NormSqr()
		{
			// Cast to F to prevent overflows
			return (X * X) + (Y * Y);
		}

		public double Norm()
		{
			return Math.Sqrt(NormSqr());
		}

		public Vector2D Mult(double d)
		{
			return new Vector2D(X * d, Y * d);
		}

		public override int  GetHashCode()
		{
			const int prime = 31;
			int result = 1;
			
			long temp = Convert.ToInt64(X);
			result = prime * result + (int)(temp ^ BitwiseHelper.ZeroFillRightShift(temp, 32));
			
			temp = Convert.ToInt64(Y);
			result = prime * result + (int)(temp ^ BitwiseHelper.ZeroFillRightShift(temp, 32));
			
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj) return true;
			if (obj == null) return false;
			if (GetType() != obj.GetType()) return false;

			var other = (Vector2D)obj;

			if (Convert.ToInt64(X) != Convert.ToInt64(other.X)) return false;

			return Convert.ToInt64(Y) == Convert.ToInt64(other.Y);
		}

	}
}