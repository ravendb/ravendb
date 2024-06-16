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
using System.Collections.Generic;
using Spatial4n.Context;
using Spatial4n.Shapes;
using Spatial4n.Util;

namespace Lucene.Net.Spatial.Prefix.Tree
{
    /// <summary>
    /// A SpatialPrefixGrid based on Geohashes.  Uses {@link GeohashUtils} to do all the geohash work.
    /// </summary>
    public class GeohashPrefixTree : SpatialPrefixTree
    {
        /// <summary>
        /// Factory for creating {@link GeohashPrefixTree} instances with useful defaults
        /// </summary>
        public class Factory : SpatialPrefixTreeFactory
        {
            protected override int GetLevelForDistance(double degrees)
            {
                var grid = new GeohashPrefixTree(ctx, GeohashPrefixTree.GetMaxLevelsPossible());
                return grid.GetLevelForDistance(degrees);
            }

            protected override SpatialPrefixTree NewSPT()
            {
                return new GeohashPrefixTree(ctx, maxLevels != null ? maxLevels.Value : GeohashPrefixTree.GetMaxLevelsPossible());
            }
        }


        public GeohashPrefixTree(SpatialContext ctx, int maxLevels)
            : base(ctx, maxLevels)
        {
            IRectangle bounds = ctx.WorldBounds;
            if (bounds.MinX != -180)
                throw new ArgumentException("Geohash only supports lat-lon world bounds. Got " + bounds);
            int MAXP = GetMaxLevelsPossible();
            if (maxLevels <= 0 || maxLevels > MAXP)
                throw new ArgumentException("maxLen must be [1-" + MAXP + "] but got " + maxLevels);

        }

        /// <summary>
        /// Any more than this and there's no point (double lat and lon are the same).
        /// </summary>
        /// <returns></returns>
        public static int GetMaxLevelsPossible()
        {
            return GeohashUtils.MaxPrecision;
        }

        public override int GetLevelForDistance(double dist)
        {
            if (dist == 0)
                return maxLevels;//short circuit
            int level = GeohashUtils.LookupHashLenForWidthHeight(dist, dist);
            return Math.Max(Math.Min(level, maxLevels), 1);
        }

        protected override Node GetNode(IPoint p, int level)
        {
            return new GhCell(GeohashUtils.EncodeLatLon(p.Y, p.X, level), this);//args are lat,lon (y,x)
        }

        public override Node GetNode(string token)
        {
            return new GhCell(token, this);
        }

        public override Node GetNode(byte[] bytes, int offset, int len)
        {
            throw new System.NotImplementedException();
        }

        public override IList<Node> GetNodes(IShape shape, int detailLevel, bool inclParents)
        {
            var s = shape as IPoint;
            return (s != null) ? base.GetNodesAltPoint(s, detailLevel, inclParents) : base.GetNodes(shape, detailLevel, inclParents);
        }

        public class GhCell : Node
        {
            public GhCell(String token, GeohashPrefixTree enclosingInstance)
                : base(enclosingInstance, token)
            {
            }

            public override void Reset(string newToken)
            {
                base.Reset(newToken);
                shape = null;
            }

            public override IList<Node> GetSubCells()
            {
                String[] hashes = GeohashUtils.GetSubGeohashes(GetGeohash());//sorted
                var cells = new List<Node>(hashes.Length);

                var enclosingInstance = (GeohashPrefixTree)spatialPrefixTree;
                foreach (String hash in hashes)
                {
                    cells.Add(new GhCell(hash, enclosingInstance));
                }
                return cells;
            }

            public override int GetSubCellsSize()
            {
                return 32;//8x4
            }

            public override Node GetSubCell(IPoint p)
            {
                return ((GeohashPrefixTree)spatialPrefixTree).GetNode(p, GetLevel() + 1); //not performant!
            }

            private IShape shape;//cache

            public override IShape GetShape()
            {
                if (shape == null)
                {
                    shape = GeohashUtils.DecodeBoundary(GetGeohash(), ((GeohashPrefixTree)spatialPrefixTree).ctx);
                }
                return shape;
            }

            public override IPoint GetCenter()
            {
                return GeohashUtils.Decode(GetGeohash(), ((GeohashPrefixTree)spatialPrefixTree).ctx);
            }

            private String GetGeohash()
            {
                return GetTokenString();
            }

        }//class GhCell
    }
}
