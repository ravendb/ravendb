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
	/// Common set of operations available on 2d shapes
	/// </summary>
	interface IGeometry2D
	{
		/// <summary>
		/// Translate according to the vector
		/// </summary>
		/// <param name="vector">The 2D vector.</param>
		void Translate(Vector2D vector);

		/// <summary>
		/// Does the shape contain the given point
		/// </summary>
		/// <param name="point">The 2D point.</param>
		/// <returns>
		/// 	<c>true</c> if [contains] [the specified point]; otherwise, <c>false</c>.
		/// </returns>
		bool Contains(Point2D point);

		/// <summary>
		/// Return the area
		/// </summary>
		double Area();

		/// <summary>
		/// Return the centroid
		/// </summary>
		Point2D Centroid();

		/// <summary>
		/// Returns information about how this shape intersects the given rectangle
		/// </summary>
		/// <param name="rectangle">The rectangle.</param>
		IntersectCase Intersect(Rectangle rectangle);
		
	}
}