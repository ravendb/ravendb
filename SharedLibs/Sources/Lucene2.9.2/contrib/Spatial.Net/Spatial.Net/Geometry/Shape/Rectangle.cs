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

namespace Lucene.Net.Spatial.Geometry.Shape
{
	public class Rectangle : IGeometry2D
	{
		public Rectangle()
		{
			MinPt = new Point2D(-1, 1);	
			MaxPt = new Point2D(1, 1);	
		}

		public Rectangle(Point2D minPt, Point2D maxPt)
		{
			MinPt = new Point2D(minPt);
			MaxPt = new Point2D(maxPt);
		}

		public Rectangle(double x1, double y1, double x2, double y2)
		{
			Set(x1, y1, x2, y2);
		}

		public Point2D MinPt { get; set; }
		public Point2D MaxPt { get; set; }

		private void Set(double x1, double y1, double x2, double y2)
		{
			MinPt = new Point2D(Math.Min(x1, x2), Math.Min(y1, y2));
			MaxPt = new Point2D(Math.Max(x1, x2), Math.Max(y1, y2));
		}

		public void Translate(Vector2D vector)
		{
			MinPt.Add(vector);
			MaxPt.Add(vector);
		}

		public bool Contains(Point2D point)
		{
			return point.X >= MinPt.X &&
			       point.X <= MaxPt.X &&
			       point.Y >= MinPt.Y &&
			       point.Y <= MaxPt.Y;
		}

		public double Area()
		{
			return (MaxPt.X - MinPt.X) * (MaxPt.Y - MinPt.Y);
		}

		public Point2D Centroid()
		{
			return new Point2D((MinPt.X + MaxPt.X) / 2, (MinPt.Y + MaxPt.Y) / 2);
		}

		public IntersectCase Intersect(Rectangle rectangle)
		{
			throw new NotImplementedException();
			//TODO
		}

		public override string ToString()
		{
			return string.Format("[{0},{1}]", MinPt, MaxPt);
		}

		public override int GetHashCode()
		{
			const int prime = 31;
			int result = 1;
			result = prime * result + ((MaxPt == null) ? 0 : MaxPt.GetHashCode());
			result = prime * result + ((MinPt == null) ? 0 : MinPt.GetHashCode());
			return result;
		}
	}
}