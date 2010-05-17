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
using System.Collections.Generic;
using System.Text;

namespace Lucene.Net.Spatial.GeoHash
{
	public class GeoHashUtils
	{
		private static readonly char[] Base32 = {
		                                        	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
		                                        	'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm',
		                                        	'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
		                                        	'y', 'z'
		                                        };

		private static readonly Dictionary<char, int> Decodemap = new Dictionary<char, int>();
		
		private const int Precision = 12;
		private static readonly int[] Bits = { 16, 8, 4, 2, 1 };


		static GeoHashUtils()
		{
			int sz = Base32.Length;
			for (int i = 0; i < sz; i++)
			{
				Decodemap[Base32[i]] = i;
			}
		}

		public static String Encode(double latitude, double longitude)
		{
			double[] latInterval = { -90.0, 90.0 };
			double[] lonInterval = { -180.0, 180.0 };

			var geohash = new StringBuilder();
			bool isEven = true;
			int bit = 0, ch = 0;

			while (geohash.Length < Precision)
			{
				double mid;
				if (isEven)
				{
					mid = (lonInterval[0] + lonInterval[1]) / 2;
					if (longitude > mid)
					{
						ch |= Bits[bit];
						lonInterval[0] = mid;
					}
					else
					{
						lonInterval[1] = mid;
					}

				}
				else
				{
					mid = (latInterval[0] + latInterval[1]) / 2;
					if (latitude > mid)
					{
						ch |= Bits[bit];
						latInterval[0] = mid;
					}
					else
					{
						latInterval[1] = mid;
					}
				}

				isEven = isEven ? false : true;

				if (bit < 4)
				{
					bit++;
				}
				else
				{
					geohash.Append(Base32[ch]);
					bit = 0;
					ch = 0;
				}
			}

			return geohash.ToString();
		}

		public static double[] Decode(String geohash)
		{
			double[] ge = DecodeExactly(geohash);
			double lat = ge[0];
			double lon = ge[1];
			double latErr = ge[2];
			double lonErr = ge[3];

			double latPrecision = Math.Max(1, Math.Round(-Math.Log10(latErr))) - 1;
			double lonPrecision = Math.Max(1, Math.Round(-Math.Log10(lonErr))) - 1;

			lat = GetPrecision(lat, latPrecision);
			lon = GetPrecision(lon, lonPrecision);

			return new [] { lat, lon };
		}

		public static double[] DecodeExactly(String geohash)
		{
			double[] latInterval = {-90.0, 90.0};
			double[] lonInterval = {-180.0, 180.0};

			double latErr = 90.0;
			double lonErr = 180.0;
			bool isEven = true;
			int sz = geohash.Length;
			int bsz = Bits.Length;
			for (int i = 0; i < sz; i++)
			{

				int cd = Decodemap[geohash[i]];

				for (int z = 0; z < bsz; z++)
				{
					int mask = Bits[z];
					if (isEven)
					{
						lonErr /= 2;
						if ((cd & mask) != 0)
						{
							lonInterval[0] = (lonInterval[0] + lonInterval[1])/2;
						}
						else
						{
							lonInterval[1] = (lonInterval[0] + lonInterval[1])/2;
						}

					}
					else
					{
						latErr /= 2;

						if ((cd & mask) != 0)
						{
							latInterval[0] = (latInterval[0] + latInterval[1])/2;
						}
						else
						{
							latInterval[1] = (latInterval[0] + latInterval[1])/2;
						}
					}
					isEven = isEven ? false : true;
				}

			}
			double latitude = (latInterval[0] + latInterval[1])/2;
			double longitude = (lonInterval[0] + lonInterval[1])/2;

			return new[] {latitude, longitude, latErr, lonErr};
		}

		public static double GetPrecision(double x, double precision)
		{
			double @base = Math.Pow(10, -precision);
			double diff = x%@base;
			return x - diff;
		}


	}
}