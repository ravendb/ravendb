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
        //Incorporated changes from https://issues.apache.org/jira/browse/LUCENENET-431
		public const String DefaltFieldPrefix = "_tier_";
        public static double EARTH_CIRC_MILES = 28892.0d;

		private readonly int _tierLevel;
		private int _tierLength;
		private int _tierBoxes;
		private readonly IProjector _projector;
		private readonly string _fieldPrefix;		

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
 	        double[] ranges = _projector.Range();

            double id = GetBoxCoord(coords[0], ranges[0]) + (GetBoxCoord(coords[1], ranges[1]) / TierVerticalPosDivider);
            return id;
		}

        private double GetBoxCoord(double coord, double range)
 	    {
 	        return Math.Floor(coord * (this._tierLength / range));
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
		public int BestFit(double range)
 	    {
 	        double times = EARTH_CIRC_MILES / (2.0d * range);
 	
 	        int bestFit = (int)Math.Ceiling(Math.Log(times, 2));
 	
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
