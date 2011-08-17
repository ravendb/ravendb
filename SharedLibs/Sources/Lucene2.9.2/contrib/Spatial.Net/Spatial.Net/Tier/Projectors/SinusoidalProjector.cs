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

namespace Lucene.Net.Spatial.Tier.Projectors
{
	/// <summary>
	/// Based on Sinusoidal Projections
	/// Project a latitude / longitude on a 2D cartesian map
	/// </summary>
	public class SinusoidalProjector : IProjector
	{
        //Incorporated changes from https://issues.apache.org/jira/browse/LUCENENET-431
        public static double LATITUDE_RANGE= Math.PI;
        public static double LONGITUDE_RANGE = 2* Math.PI;

		public string CoordsAsString(double latitude, double longitude)
		{
            return null;
		}

		public double[] Coords(double latitude, double longitude)
		{
			double rlat = MathHelper.ToRadians(latitude);
			double rlong = MathHelper.ToRadians(longitude);
			double nlat = rlong * Math.Cos(rlat);
			double[] r = {nlat, rlong};
			return r;
		}

        public double[] Range() 
        {
            double[] ranges = { LATITUDE_RANGE, LONGITUDE_RANGE };
            return ranges;
        } 
	}
}
