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
using Lucene.Net.Search;
using Lucene.Net.Spatial.Geometry;
using Lucene.Net.Spatial.Geometry.Shape;
using Lucene.Net.Spatial.Tier.Projectors;

namespace Lucene.Net.Spatial.Tier
{
	public class CartesianPolyFilterBuilder
	{
		/// <summary>
		/// Finer granularity than 1 mile isn't accurate with
		/// standard C# math.  Also, there's already a 2nd
		/// precise filter, if needed, in DistanceQueryBuilder,
		/// that will make the filtering exact.
		/// </summary>
		public static double MilesFloor = 1.0;

		private readonly IProjector _projector = new SinusoidalProjector();
		private readonly string _tierPrefix;

		public CartesianPolyFilterBuilder(string tierPrefix)
		{
			_tierPrefix = tierPrefix;
		}

		public Shape GetBoxShape(double latitude, double longitude, double miles)
		{
			if (miles < MilesFloor)
			{
				miles = MilesFloor;
			}
			//Rectangle box = DistanceUtils.GetInstance().GetBoundary(latitude, longitude, miles);
			LLRect box1 = LLRect.CreateBox(new FloatLatLng(latitude, longitude), miles, miles);
			LatLng ll = box1.GetLowerLeft();
			LatLng ur = box1.GetUpperRight();

			double latY = ur.GetLat();
			double latX = ll.GetLat();
			double longY = ur.GetLng();
			double longX = ll.GetLng();
			double longX2 = 0.0;

			if (ur.GetLng() < 0.0 && ll.GetLng() > 0.0)
			{
				longX2 = ll.GetLng();
				longX = -180.0;
			}
			if (ur.GetLng() > 0.0 && ll.GetLng() < 0.0)
			{
				longX2 = ll.GetLng();
				longX = 0.0;
			}

			var ctp = new CartesianTierPlotter(2, _projector, _tierPrefix);
			int bestFit = ctp.BestFit(miles);

			ctp = new CartesianTierPlotter(bestFit, _projector, _tierPrefix);
			
			var shape = new Shape(ctp.GetTierFieldName());

			// generate shape
			// iterate from startX->endX
			// iterate from startY -> endY
			// shape.add(currentLat.currentLong);

			shape = GetShapeLoop(shape, ctp, latX, longX, latY, longY);
			if (longX2 != 0.0)
			{
				if (longX2 != 0.0)
				{
					if (longX == 0.0)
					{
						longX = longX2;
						longY = 0.0;
						shape = GetShapeLoop(shape, ctp, latX, longX, latY, longY);
					}
					else
					{
						longX = longX2;
						longY = -180.0;
						shape = GetShapeLoop(shape, ctp, latY, longY, latX, longX);
					}
				}
			}

			return shape;
		}

		public Shape GetShapeLoop(Shape shape, CartesianTierPlotter ctp, double latX, double longX, double latY, double longY)
		{
			double beginAt = ctp.GetTierBoxId(latX, longX);
			double endAt = ctp.GetTierBoxId(latY, longY);

			double tierVert = ctp.TierVerticalPosDivider;
			
			double startX = beginAt - (beginAt % 1);
			double startY = beginAt - startX; //should give a whole number

			double endX = endAt - (endAt % 1);
			double endY = endAt - endX; //should give a whole number

			int scale = (int) Math.Log10(tierVert);

			endY = Math.Round(endY, scale, MidpointRounding.ToEven);
			startY = Math.Round(startY, scale, MidpointRounding.ToEven);

			double xInc = 1.0d / tierVert;
			xInc = Math.Round(xInc, scale, MidpointRounding.ToEven);
			
			for (; startX <= endX; startX++)
			{
				double itY = startY;
				
				while (itY <= endY)
				{
					//create a boxId
					// startX.startY
					double boxId = startX + itY;
					shape.AddBox(boxId);

					itY += Math.Round(xInc, scale, MidpointRounding.ToEven);
				}
			}
			return shape;
		}

		public Filter GetBoundingArea(double latitude, double longitude, double miles)
		{
			Shape shape = GetBoxShape(latitude, longitude, miles);
			return new CartesianShapeFilter(shape, shape.TierId);
		}
	}
}