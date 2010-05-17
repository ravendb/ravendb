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

using Lucene.Net.Spatial.Geometry;
using Lucene.Net.Spatial.Geometry.Shape;

namespace Lucene.Net.Spatial.Tier
{
	public class DistanceUtils
	{
		private static readonly DistanceUtils Instance = new DistanceUtils();

		public static DistanceUtils GetInstance()
		{
			return Instance;
		}

		public double GetDistanceMi(double x1, double y1, double x2, double y2)
		{
			return GetLLMDistance(x1, y1, x2, y2);
		}

		public Rectangle GetBoundary(double x1, double y1, double miles)
		{
			var box = LLRect.CreateBox(new FloatLatLng(x1, y1), miles, miles);
			return box.ToRectangle();
		}

		public double GetLLMDistance(double x1, double y1, double x2, double y2)
		{

			LatLng p1 = new FloatLatLng(x1, y1);
			LatLng p2 = new FloatLatLng(x2, y2);
			return p1.ArcDistance(p2, DistanceUnits.MILES);
		}
	}
}