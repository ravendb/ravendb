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

using Lucene.Net.Search;
using Spatial4n.Context;
using Spatial4n.Distance;
using Spatial4n.Shapes;

namespace Lucene.Net.Spatial.BBox
{
    /// <summary>
    /// Returns the distance between the center of the indexed rectangle and the
    /// query shape.
    /// </summary>
    public class DistanceSimilarity : BBoxSimilarity
    {
        private readonly IPoint queryPoint;
        private readonly IDistanceCalculator distCalc;
        private readonly double nullValue;

        public DistanceSimilarity(SpatialContext ctx, IPoint queryPoint)
        {
            this.queryPoint = queryPoint;
            this.distCalc = ctx.DistanceCalculator;
            this.nullValue = (ctx.IsGeo ? 180 : double.MaxValue);
        }

        public double Score(IRectangle indexRect, Explanation exp)
        {
            double score;
            if (indexRect == null)
            {
                score = nullValue;
            }
            else
            {
                score = distCalc.Distance(queryPoint, indexRect.Center);
            }
            if (exp != null)
            {
                exp.Value = (float)score;
                exp.Description = GetType().Name;
                exp.AddDetail(new Explanation(-1f, "" + queryPoint));
                exp.AddDetail(new Explanation(-1f, "" + indexRect));
            }
            return score;
        }
    }
}