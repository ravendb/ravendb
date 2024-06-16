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

using Lucene.Net.Index;
using Lucene.Net.Spatial.Prefix.Tree;
using Lucene.Net.Spatial.Util;
using Spatial4n.Shapes;
using System;

namespace Lucene.Net.Spatial.Prefix
{
    /// <summary>
    /// Implementation of {@link ShapeFieldCacheProvider} designed for {@link PrefixTreeStrategy}s.
    ///
    /// Note, due to the fragmented representation of Shapes in these Strategies, this implementation
    /// can only retrieve the central {@link Point} of the original Shapes.
    /// </summary>
	public class PointPrefixTreeFieldCacheProvider : ShapeFieldCacheProvider<IPoint>
    {
        private readonly SpatialPrefixTree grid; //

        public PointPrefixTreeFieldCacheProvider(SpatialPrefixTree grid, String shapeField, int defaultSize)
            : base(shapeField, defaultSize)
        {
            this.grid = grid;
        }

        //A kluge that this is a field
        private Node scanCell = null;

        protected override IPoint ReadShape(Term term)
        {
            scanCell = grid.GetNode(term.Text, scanCell);
            return scanCell.IsLeaf() ? scanCell.GetShape().Center : null;
        }
    }
}