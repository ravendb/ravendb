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
	public class Ellipse : IGeometry2D
	{
		private readonly Point2D _center;
		// Half length of major axis
		private readonly double _a;
		// Half length of minor axis
		private readonly double _b;

		private readonly double _k1;
		private readonly double _k2;
		private readonly double _k3;

		// sin of rotation angle
		private readonly double _s;
		// cos of rotation angle
		private readonly double _c;

		public Ellipse()
		{
			_center = new Point2D(0, 0);
		}

		/// <summary>
		/// Constructor given bounding rectangle and a rotation.
		/// </summary>
		/// <param name="p1">The point 1.</param>
		/// <param name="p2">The point 2.</param>
		/// <param name="angle">The angle.</param>
		public Ellipse(Point2D p1, Point2D p2, double angle)
		{
			// Set the center
			_center = new Point2D
			          	{
			          		X = (p1.X + p2.X)*0.5f, 
			          		Y = (p1.Y + p2.Y)*0.5f
			          	};

            
			// Find sin and cos of the angle
			double angleRad = MathHelper.ToRadians(angle);
			_c = Math.Cos(angleRad);
			_s = Math.Sin(angleRad);

			// Find the half lengths of the semi-major and semi-minor axes
			double dx = Math.Abs(p2.X - p1.X) * 0.5;
			double dy = Math.Abs(p2.Y - p1.Y) * 0.5;
			
			if (dx >= dy)
			{
				_a = dx;
				_b = dy;
			}
			else
			{
				_a = dy;
				_b = dx;
			}

			// Find _k1, _k2, _k3 - define when a point x,y is on the ellipse
			_k1 = Sqr(_c / _a) + Sqr(_s / _b);
			_k2 = 2 * _s * _c * ((1 / Sqr(_a)) - (1 / Sqr(_b)));
			_k3 = Sqr(_s / _a) + Sqr(_c / _b);
		}

		private static double Sqr(double d)
		{
			return d * d;
		}

		public int Intersect(LineSegment seg, Point2D pt0, Point2D pt1)
		{
			if (pt0 == null)
				pt0 = new Point2D();
			if (pt1 == null)
				pt1 = new Point2D();

			// Solution is found by parameterizing the line segment and
			// substituting those values into the ellipse equation.
			// Results in a quadratic equation.
			double x1 = _center.X;
			double y1 = _center.Y;
			double u1 = seg.A.X;
			double v1 = seg.A.Y;
			double u2 = seg.B.X;
			double v2 = seg.B.Y;
			double dx = u2 - u1;
			double dy = v2 - v1;
			double q0 = _k1 * Sqr(u1 - x1) + _k2 * (u1 - x1) * (v1 - y1) + _k3
			                                                               * Sqr(v1 - y1) - 1;
			double q1 = (2 * _k1 * dx * (u1 - x1)) + (_k2 * dx * (v1 - y1))
			            + (_k2 * dy * (u1 - x1)) + (2 * _k3 * dy * (v1 - y1));
			double q2 = (_k1 * Sqr(dx)) + (_k2 * dx * dy) + (_k3 * Sqr(dy));

			// Compare q1^2 to 4*q0*q2 to see how quadratic solves
			double d = Sqr(q1) - (4 * q0 * q2);
			if (d < 0)
			{
				// Roots are complex valued. Line containing the segment does
				// not intersect the ellipse
				return 0;
			}

			if (d == 0)
			{
				// One real-valued root - line is tangent to the ellipse
				double t = -q1 / (2 * q2);
				if (0 <= t && t <= 1)
				{
					// Intersection occurs along line segment
					pt0.X = u1 + t * dx;
					pt0.Y = v1 + t * dy;
					return 1;
				}
				
				return 0;
			}
			else
			{
				// Two distinct real-valued roots. Solve for the roots and see if
				// they fall along the line segment
				int n = 0;
				double q = Math.Sqrt(d);
				double t = (-q1 - q) / (2 * q2);
				if (0 <= t && t <= 1)
				{
					// Intersection occurs along line segment
					pt0.X = u1 + t * dx;
					pt0.Y = v1 + t * dy;
					n++;
				}

				// 2nd root
				t = (-q1 + q) / (2 * q2);
				if (0 <= t && t <= 1)
				{
					if (n == 0)
					{
						pt0.X = u1 + t * dx;
						pt0.Y = v1 + t * dy;
						n++;
					}
					else
					{
						pt1.X = u1 + t * dx;
						pt1.Y = v1 + t * dy;
						n++;
					}
				}
				return n;
			}
		}

		public void Translate(Vector2D vector)
		{
			throw new NotSupportedException();
		}

		public bool Contains(Point2D point)
		{
			// Plug in equation for ellipse, If evaluates to <= 0 then the
			// point is in or on the ellipse.
			double dx = point.X - _center.X;
			double dy = point.Y - _center.Y;
			double eq = (((_k1 * Sqr(dx)) + (_k2 * dx * dy) + (_k3 * Sqr(dy)) - 1));

			return eq <= 0;
		}

		public double Area()
		{
			throw new NotSupportedException();
		}

		public Point2D Centroid()
		{
			throw new NotSupportedException();
		}

		public IntersectCase Intersect(Rectangle rectangle)
		{
			// Test if all 4 corners of the rectangle are inside the ellipse
			var ul = new Point2D(rectangle.MinPt.X, rectangle.MaxPt.Y);
			var ur = new Point2D(rectangle.MaxPt.X, rectangle.MaxPt.Y);
			var ll = new Point2D(rectangle.MinPt.X, rectangle.MinPt.Y);
			var lr = new Point2D(rectangle.MaxPt.X, rectangle.MinPt.Y);
			
			if (Contains(ul) && Contains(ur) && Contains(ll) && Contains(lr)) return IntersectCase.CONTAINS;

			// Test if any of the rectangle edges intersect
			Point2D pt0 = new Point2D(), pt1 = new Point2D();

			var bottom = new LineSegment(ll, lr);

			if (Intersect(bottom, pt0, pt1) > 0)
				return IntersectCase.INTERSECTS;

			var top = new LineSegment(ul, ur);
			if (Intersect(top, pt0, pt1) > 0)
				return IntersectCase.INTERSECTS;

			var left = new LineSegment(ll, ul);
			if (Intersect(left, pt0, pt1) > 0)
				return IntersectCase.INTERSECTS;

			var right = new LineSegment(lr, ur);
			if (Intersect(right, pt0, pt1) > 0)
				return IntersectCase.INTERSECTS;

			// Ellipse does not intersect any edge : since the case for the ellipse
			// containing the rectangle was considered above then if the center
			// is inside the ellipse is fully inside and if center is outside
			// the ellipse is fully outside
			return (rectangle.Contains(_center)) ? IntersectCase.WITHIN : IntersectCase.OUTSIDE;
		}
	}
}