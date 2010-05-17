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

namespace Lucene.Net.Spatial.Geometry.Shape
{
	/// <summary>
	/// 2D line segment
	/// </summary>
	public class LineSegment
	{
		public readonly Point2D A = new Point2D();
		public readonly Point2D B = new Point2D();

		public LineSegment()
		{
			A.Set(0, 0);
			B.Set(0, 0);
		}

		public LineSegment(Point2D p1, Point2D p2)
		{
			A.Set(p1);
			B.Set(p2);
		}

		/// <summary>
		/// Finds the distance of a specified point from the line segment and the
		/// closest point on the segment to the specified point.
		/// </summary>
		/// <param name="p">The test point.</param>
		/// <param name="closestPt">Closest point on the segment to c.</param>
		/// <returns>Returns the distance from p to the closest point on the segment.</returns>
		public double Distance(Point2D p, Point2D closestPt)
		{
			if (closestPt == null)
				closestPt = new Point2D();

			// Construct vector v (AB) and w (AP)
			var v = new Vector2D(A, B);
			var w = new Vector2D(A, p);

			// Numerator of the component of w onto v. If <= 0 then A
			// is the closest point. By separating into the numerator
			// and denominator of the component we avoid a division unless
			// it is necessary.
			double n = w.Dot(v);
			if (n <= 0.0f)
			{
				closestPt.Set(A);
				return w.Norm();
			}

			// Get the denominator of the component. If the component >= 1
			// (d <= n) then point B is the closest point
			double d = v.Dot(v);
			if (d <= n)
			{
				closestPt.Set(B);
				return new Vector2D(B, p).Norm();
			}

			// Closest point is along the segment. The point is the projection of
			// w onto v.
			closestPt.Set(v.Mult(n / d));
			closestPt.Add(A);
			return new Vector2D(closestPt, p).Norm();
		}

		public override int GetHashCode()
		{
			const int prime = 31;
			int result = 1;
			result = prime * result + ((A == null) ? 0 : A.GetHashCode());
			result = prime * result + ((B == null) ? 0 : B.GetHashCode());
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj) return true;
			if (obj == null) return false;
			if (GetType() != obj.GetType()) return false;
			
			var other = (LineSegment)obj;
			
			if (A == null)
			{
				if (other.A != null) return false;
			}
			else if (!A.Equals(other.A)) return false;
			
			if (B == null)
			{
				if (other.B != null) return false;
			}
			else if (!B.Equals(other.B)) return false;

			return true;
		}
	}
}