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

namespace Lucene.Net.Spatial.Tier.Projectors
{
	public class CartesianTierPlotter
	{
		public const String DefaltFieldPrefix = "_tier_";

		private readonly int _tierLevel;
		private int _tierLength;
		private int _tierBoxes;
		private readonly IProjector _projector;
		private readonly string _fieldPrefix;
		private const double Idd = 180d;

		public CartesianTierPlotter(int tierLevel, IProjector projector, string fieldPrefix)
		{
			_tierLevel = tierLevel;
			_fieldPrefix = fieldPrefix;
			_projector = projector;

			SetTierLength();
			SetTierBoxes();
			SetTierVerticalPosDivider();
		}

		public int TierVerticalPosDivider{ get; private set; }

		private void SetTierLength()
		{
			_tierLength = (int)Math.Pow(2, _tierLevel);
		}

		private void SetTierBoxes()
		{
			_tierBoxes = (int)Math.Pow(_tierLength, 2);
		}

		private void SetTierVerticalPosDivider()
		{
			// ceiling of log base 10 of tierLen
			TierVerticalPosDivider = Convert.ToInt32(Math.Ceiling(Math.Log10(_tierLength)));
			// 
			TierVerticalPosDivider = (int)Math.Pow(10, TierVerticalPosDivider);
		}

		/// <summary>
		/// TierBoxId is latitude box id + longitude box id
		/// where latitude box id, and longitude box id are transposed in to position
		/// coordinates.
		/// </summary>
		/// <param name="latitude">The latitude.</param>
		/// <param name="longitude">The longitude.</param>
		/// <returns></returns>
		public double GetTierBoxId(double latitude, double longitude)
		{
			double[] coords = _projector.Coords(latitude, longitude);

			double id = GetBoxId(coords[0]) + (GetBoxId(coords[1]) / TierVerticalPosDivider);
			return id;
		}

		private double GetBoxId(double coord)
		{
			return Math.Floor(coord / (Idd / _tierLength));
		}

		private double GetBoxId(double coord, int tierLen)
		{
			return Math.Floor(coord / (Idd / tierLen));
		}

		/// <summary>
		/// Get the string name representing current tier _localTier&lt;tiedId&gt;
		/// </summary>
		public String GetTierFieldName()
		{
			return _fieldPrefix + _tierLevel;
		}

		/// <summary>
		/// Get the string name representing tierId _localTier&lt;tierId&gt;
		/// </summary>
		/// <param name="tierId">The tier id.</param>
		public string GetTierFieldName(int tierId)
		{
			return _fieldPrefix + tierId;
		}

		/// <summary>
		/// Find the tier with the best fit for a bounding box
		/// Best fit is defined as the ceiling of
		/// log2 (circumference of earth / distance) 
		/// distance is defined as the smallest box fitting
		/// the corner between a radius and a bounding box.
		///
		/// Distances less than a mile return 15, finer granularity is
		/// in accurate
		/// </summary>
		/// <param name="miles">The miles.</param>
		/// <returns></returns>
		public int BestFit(double miles)
		{

			//28,892 a rough circumference of the earth
			const int circ = 28892;

			double r = miles / 2.0;

			double corner = r - Math.Sqrt(Math.Pow(r, 2) / 2.0d);
			double times = circ / corner;
			int bestFit = (int)Math.Ceiling(Log2(times)) + 1;

			if (bestFit > 15)
			{
				// 15 is the granularity of about 1 mile
				// finer granularity isn't accurate with standard java math
				return 15;
			}
			return bestFit;
		}

		/// <summary>
		/// A log to the base 2 formula.
		/// <code>Math.Log(value) / Math.Log(2)</code>
		/// </summary>
		/// <param name="value">The value.</param>
		public double Log2(double value)
		{
			return Math.Log(value) / Math.Log(2);
		}
	}
}