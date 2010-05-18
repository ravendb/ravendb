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
	/// Point class.  This type is mutable.
	/// </summary>
	public class Point2D
	{
		public Point2D()
		{
			X = 0;
			Y = 0;
		}

		public Point2D(double x, double y)
		{
			X = x;
			Y = y;
		}

		public Point2D(Point2D other)
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

		public void Add(Vector2D v)
		{
			X += v.X;
			Y += v.Y;
		}

		public void Set(Point2D p1)
		{
			X = p1.X;
			Y = p1.Y;
		}

		public void Add(Point2D a)
		{
			X += a.X;
			Y += a.Y;
		}

		public void Set(Vector2D v)
		{
			X = v.X;
			Y = v.Y;
		}

		public override string ToString()
		{
			return string.Format("({0},{1})", X, Y);
		}

		public override int GetHashCode()
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
			
			var other = (Point2D)obj;

			if (Convert.ToInt64(X) != Convert.ToInt64(other.X)) return false;
			
			return Convert.ToInt64(Y) == Convert.ToInt64(other.Y);
		}

		
		
		
	}
}