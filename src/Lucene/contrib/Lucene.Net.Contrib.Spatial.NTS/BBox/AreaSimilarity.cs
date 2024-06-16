/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using Lucene.Net.Search;

using Spatial4n.Shapes;
namespace Lucene.Net.Spatial.BBox
{
	/// <summary>
    /// The algorithm is implemented as envelope on envelope overlays rather than
    /// complex polygon on complex polygon overlays.
    /// <p/>
    /// <p/>
    /// Spatial relevance scoring algorithm:
    /// <p/>
    /// <br/>  queryArea = the area of the input query envelope
    /// <br/>  targetArea = the area of the target envelope (per Lucene document)
    /// <br/>  intersectionArea = the area of the intersection for the query/target envelopes
    /// <br/>  queryPower = the weighting power associated with the query envelope (default = 1.0)
    /// <br/>  targetPower =  the weighting power associated with the target envelope (default = 1.0)
    /// <p/>
    /// <br/>  queryRatio  = intersectionArea / queryArea;
    /// <br/>  targetRatio = intersectionArea / targetArea;
    /// <br/>  queryFactor  = Math.pow(queryRatio,queryPower);
    /// <br/>  targetFactor = Math.pow(targetRatio,targetPower);
    /// <br/>  score = queryFactor /// targetFactor;
    /// <p/>
    /// Based on Geoportal's
    /// <a href="http://geoportal.svn.sourceforge.net/svnroot/geoportal/Geoportal/trunk/src/com/esri/gpt/catalog/lucene/SpatialRankingValueSource.java">
    ///   SpatialRankingValueSource</a>.
    ///
    /// @lucene.experimental
	/// </summary>
	public class AreaSimilarity : BBoxSimilarity
	{
	   /*
		* Properties associated with the query envelope
		*/
		private readonly IRectangle queryExtent;
		private readonly double queryArea;

		private readonly double targetPower;
		private readonly double queryPower;

		public AreaSimilarity(IRectangle queryExtent, double queryPower, double targetPower)
		{
			this.queryExtent = queryExtent;
			this.queryArea = queryExtent.GetArea(null);

			this.queryPower = queryPower;
			this.targetPower = targetPower;

			//  if (this.qryMinX > queryExtent.getMaxX()) {
			//    this.qryCrossedDateline = true;
			//    this.qryArea = Math.abs(qryMaxX + 360.0 - qryMinX) * Math.abs(qryMaxY - qryMinY);
			//  } else {
			//    this.qryArea = Math.abs(qryMaxX - qryMinX) * Math.abs(qryMaxY - qryMinY);
			//  }
		}

		public AreaSimilarity(IRectangle queryExtent)
			: this(queryExtent, 2.0, 0.5)
		{
		}

		public String GetDelimiterQueryParameters()
		{
			return queryExtent + ";" + queryPower + ";" + targetPower;
		}

		public double Score(IRectangle target, Explanation exp)
		{
			if (target == null || queryArea <= 0)
			{
				return 0;
			}
			double targetArea = target.GetArea(null);
			if (targetArea <= 0)
			{
				return 0;
			}
			double score = 0;

			double top = Math.Min(queryExtent.MaxY, target.MaxY);
			double bottom = Math.Max(queryExtent.MinY, target.MinY);
			double height = top - bottom;
			double width = 0;

			// queries that cross the date line
			if (queryExtent.CrossesDateLine)
			{
				// documents that cross the date line
				if (target.CrossesDateLine)
				{
					double left = Math.Max(queryExtent.MinX, target.MinX);
					double right = Math.Min(queryExtent.MaxX, target.MaxX);
					width = right + 360.0 - left;
				}
				else
				{
					double qryWestLeft = Math.Max(queryExtent.MinX, target.MaxX);
					double qryWestRight = Math.Min(target.MaxX, 180.0);
					double qryWestWidth = qryWestRight - qryWestLeft;
					if (qryWestWidth > 0)
					{
						width = qryWestWidth;
					}
					else
					{
						double qryEastLeft = Math.Max(target.MaxX, -180.0);
						double qryEastRight = Math.Min(queryExtent.MaxX, target.MaxX);
						double qryEastWidth = qryEastRight - qryEastLeft;
						if (qryEastWidth > 0)
						{
							width = qryEastWidth;
						}
					}
				}
			}
			else
			{ // queries that do not cross the date line

				if (target.CrossesDateLine)
				{
					double tgtWestLeft = Math.Max(queryExtent.MinX, target.MinX);
					double tgtWestRight = Math.Min(queryExtent.MaxX, 180.0);
					double tgtWestWidth = tgtWestRight - tgtWestLeft;
					if (tgtWestWidth > 0)
					{
						width = tgtWestWidth;
					}
					else
					{
						double tgtEastLeft = Math.Max(queryExtent.MinX, -180.0);
						double tgtEastRight = Math.Min(queryExtent.MaxX, target.MaxX);
						double tgtEastWidth = tgtEastRight - tgtEastLeft;
						if (tgtEastWidth > 0)
						{
							width = tgtEastWidth;
						}
					}
				}
				else
				{
					double left = Math.Max(queryExtent.MinX, target.MinX);
					double right = Math.Min(queryExtent.MaxX, target.MaxX);
					width = right - left;
				}
			}


			// calculate the score
			if ((width > 0) && (height > 0))
			{
				double intersectionArea = width * height;
				double queryRatio = intersectionArea / queryArea;
				double targetRatio = intersectionArea / targetArea;
				double queryFactor = Math.Pow(queryRatio, queryPower);
				double targetFactor = Math.Pow(targetRatio, targetPower);
				score = queryFactor * targetFactor * 10000.0;

				if (exp != null)
				{
					//        StringBuilder sb = new StringBuilder();
					//        sb.append("\nscore=").append(score);
					//        sb.append("\n  query=").append();
					//        sb.append("\n  target=").append(target.toString());
					//        sb.append("\n  intersectionArea=").append(intersectionArea);
					//        
					//        sb.append(" queryArea=").append(queryArea).append(" targetArea=").append(targetArea);
					//        sb.append("\n  queryRatio=").append(queryRatio).append(" targetRatio=").append(targetRatio);
					//        sb.append("\n  queryFactor=").append(queryFactor).append(" targetFactor=").append(targetFactor);
					//        sb.append(" (queryPower=").append(queryPower).append(" targetPower=").append(targetPower).append(")");

					exp.Value = (float) score;
					exp.Description = GetType().Name;

					Explanation e = null;

					exp.AddDetail(e = new Explanation((float)intersectionArea, "IntersectionArea"));
					e.AddDetail(new Explanation((float)width, "width; Query: " + queryExtent));
					e.AddDetail(new Explanation((float)height, "height; Target: " + target));

					exp.AddDetail(e = new Explanation((float)queryFactor, "Query"));
					e.AddDetail(new Explanation((float)queryArea, "area"));
					e.AddDetail(new Explanation((float)queryRatio, "ratio"));
					e.AddDetail(new Explanation((float)queryPower, "power"));

					exp.AddDetail(e = new Explanation((float)targetFactor, "Target"));
					e.AddDetail(new Explanation((float)targetArea, "area"));
					e.AddDetail(new Explanation((float)targetRatio, "ratio"));
					e.AddDetail(new Explanation((float)targetPower, "power"));
				}
			}
			else if (exp != null)
			{
				exp.Value = 0;
				exp.Description = "Shape does not intersect";
			}
			return score;
		}

		public override bool Equals(object obj)
		{
			var other = obj as AreaSimilarity;
			if (other == null) return false;
			return GetDelimiterQueryParameters().Equals(other.GetDelimiterQueryParameters());
		}

		public override int GetHashCode()
		{
			return GetDelimiterQueryParameters().GetHashCode();
		} 
	}
}
