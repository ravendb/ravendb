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
using System.Diagnostics;
using System.Text;
using Spatial4n.Context;
using Spatial4n.Shapes;

namespace Lucene.Net.Spatial.Prefix.Tree
{
    /// <summary>
    /// Implementation of {@link SpatialPrefixTree} which uses a quad tree
    /// (http://en.wikipedia.org/wiki/Quadtree)
    /// </summary>
	public class QuadPrefixTree : SpatialPrefixTree
	{
        /// <summary>
        /// Factory for creating {@link QuadPrefixTree} instances with useful defaults
        /// </summary>
		public class Factory : SpatialPrefixTreeFactory
		{
			protected override int GetLevelForDistance(double degrees)
			{
				var grid = new QuadPrefixTree(ctx, MAX_LEVELS_POSSIBLE);
				return grid.GetLevelForDistance(degrees);
			}

			protected override SpatialPrefixTree NewSPT()
			{
				return new QuadPrefixTree(ctx, maxLevels != null ? maxLevels.Value : MAX_LEVELS_POSSIBLE);
			}
		}

		public static readonly int MAX_LEVELS_POSSIBLE = 50;//not really sure how big this should be

		public static readonly int DEFAULT_MAX_LEVELS = 12;
		private double xmin;
		private double xmax;
		private double ymin;
		private double ymax;
		private double xmid;
		private double ymid;

		private double gridW;
		private double gridH;

		double[] levelW;
		double[] levelH;
		int[] levelS; // side
		int[] levelN; // number

		public QuadPrefixTree(SpatialContext ctx, IRectangle bounds, int maxLevels)
			: base(ctx, maxLevels)
		{
			Init(ctx, bounds, maxLevels);
		}

		public QuadPrefixTree(SpatialContext ctx)
			: base(ctx, DEFAULT_MAX_LEVELS)
		{
			Init(ctx, ctx.WorldBounds, DEFAULT_MAX_LEVELS);
		}

		public QuadPrefixTree(SpatialContext ctx, int maxLevels)
			: base(ctx, maxLevels)
		{
			Init(ctx, ctx.WorldBounds, maxLevels);
		}

		protected void Init(SpatialContext ctx, IRectangle bounds, int maxLevels)
		{
			this.xmin = bounds.MinX;
			this.xmax = bounds.MaxX;
			this.ymin = bounds.MinY;
			this.ymax = bounds.MaxY;

			levelW = new double[maxLevels];
			levelH = new double[maxLevels];
			levelS = new int[maxLevels];
			levelN = new int[maxLevels];

			gridW = xmax - xmin;
			gridH = ymax - ymin;
			xmid = xmin + gridW / 2.0;
			ymid = ymin + gridH / 2.0;
			levelW[0] = gridW / 2.0;
			levelH[0] = gridH / 2.0;
			levelS[0] = 2;
			levelN[0] = 4;

			for (int i = 1; i < levelW.Length; i++)
			{
				levelW[i] = levelW[i - 1] / 2.0;
				levelH[i] = levelH[i - 1] / 2.0;
				levelS[i] = levelS[i - 1] * 2;
				levelN[i] = levelN[i - 1] * 4;
			}

		}

		public override int GetLevelForDistance(double dist)
		{
            if (dist == 0)//short circuit
                return maxLevels;
            for (int i = 0; i < maxLevels - 1; i++)
			{
				//note: level[i] is actually a lookup for level i+1
                if (dist > levelW[i] && dist > levelH[i])
				{
					return i + 1;
				}
			}
			return maxLevels;
		}

		protected override Node GetNode(IPoint p, int level)
		{
			var cells = new List<Node>(1);
            Build(xmid, ymid, 0, cells, new StringBuilder(), ctx.MakePoint(p.X, p.Y), level);
			return cells[0];//note cells could be longer if p on edge
		}

		public override Node GetNode(string token)
		{
			return new QuadCell(token, this);
		}

		public override Node GetNode(byte[] bytes, int offset, int len)
		{
			throw new System.NotImplementedException();
		}

		public override IList<Node> GetNodes(IShape shape, int detailLevel, bool inclParents)
		{
			var point = shape as IPoint;
			if (point != null)
				return base.GetNodesAltPoint(point, detailLevel, inclParents);
			else
				return base.GetNodes(shape, detailLevel, inclParents);
		}

		private void Build(double x, double y, int level, List<Node> matches, StringBuilder str, IShape shape, int maxLevel)
		{
			Debug.Assert(str.Length == level);
			double w = levelW[level] / 2;
			double h = levelH[level] / 2;

			// Z-Order
			// http://en.wikipedia.org/wiki/Z-order_%28curve%29
			CheckBattenberg('A', x - w, y + h, level, matches, str, shape, maxLevel);
			CheckBattenberg('B', x + w, y + h, level, matches, str, shape, maxLevel);
			CheckBattenberg('C', x - w, y - h, level, matches, str, shape, maxLevel);
			CheckBattenberg('D', x + w, y - h, level, matches, str, shape, maxLevel);

			// possibly consider hilbert curve
			// http://en.wikipedia.org/wiki/Hilbert_curve
			// http://blog.notdot.net/2009/11/Damn-Cool-Algorithms-Spatial-indexing-with-Quadtrees-and-Hilbert-Curves
			// if we actually use the range property in the query, this could be useful
		}

		private void CheckBattenberg(
			char c,
			double cx,
			double cy,
			int level,
			List<Node> matches,
			StringBuilder str,
			IShape shape,
			int maxLevel)
		{
			Debug.Assert(str.Length == level);
			double w = levelW[level] / 2;
			double h = levelH[level] / 2;

			int strlen = str.Length;
            IRectangle rectangle = ctx.MakeRectangle(cx - w, cx + w, cy - h, cy + h);
            SpatialRelation v = shape.Relate(rectangle);
			if (SpatialRelation.Contains == v)
			{
				str.Append(c);
				//str.append(SpatialPrefixGrid.COVER);
				matches.Add(new QuadCell(str.ToString(), v.Transpose(), this));
			}
			else if (SpatialRelation.Disjoint == v)
			{
				// nothing
			}
			else
			{ // SpatialRelation.WITHIN, SpatialRelation.INTERSECTS
				str.Append(c);

				int nextLevel = level + 1;
				if (nextLevel >= maxLevel)
				{
					//str.append(SpatialPrefixGrid.INTERSECTS);
					matches.Add(new QuadCell(str.ToString(), v.Transpose(), this));
				}
				else
				{
					Build(cx, cy, nextLevel, matches, str, shape, maxLevel);
				}
			}
			str.Length = strlen;
		}

		public class QuadCell : Node
		{

			public QuadCell(String token, QuadPrefixTree enclosingInstance)
				: base(enclosingInstance, token)
			{
			}

			public QuadCell(String token, SpatialRelation shapeRel, QuadPrefixTree enclosingInstance)
				: base(enclosingInstance, token)
			{
				this.shapeRel = shapeRel;
			}

			public override void Reset(string newToken)
			{
				base.Reset(newToken);
				shape = null;
			}

			public override IList<Node> GetSubCells()
			{
				var tree = (QuadPrefixTree)spatialPrefixTree;
				var cells = new List<Node>(4)
                  	{
                  		new QuadCell(GetTokenString() + "A", tree),
                  		new QuadCell(GetTokenString() + "B", tree),
                  		new QuadCell(GetTokenString() + "C", tree),
                  		new QuadCell(GetTokenString() + "D", tree)
                  	};
				return cells;
			}

			public override int GetSubCellsSize()
			{
				return 4;
			}

			public override Node GetSubCell(IPoint p)
			{
				return ((QuadPrefixTree)spatialPrefixTree).GetNode(p, GetLevel() + 1); //not performant!
			}

			private IShape shape;//cache

			public override IShape GetShape()
			{
				if (shape == null)
					shape = MakeShape();
				return shape;
			}

			private IRectangle MakeShape()
			{
				String token = GetTokenString();
				var tree = ((QuadPrefixTree)spatialPrefixTree);
				double xmin = tree.xmin;
				double ymin = tree.ymin;

				for (int i = 0; i < token.Length; i++)
				{
					char c = token[i];
					if ('A' == c || 'a' == c)
					{
						ymin += tree.levelH[i];
					}
					else if ('B' == c || 'b' == c)
					{
						xmin += tree.levelW[i];
						ymin += tree.levelH[i];
					}
					else if ('C' == c || 'c' == c)
					{
						// nothing really
					}
					else if ('D' == c || 'd' == c)
					{
						xmin += tree.levelW[i];
					}
					else
					{
						throw new Exception("unexpected char: " + c);
					}
				}
				int len = token.Length;
				double width, height;
				if (len > 0)
				{
					width = tree.levelW[len - 1];
					height = tree.levelH[len - 1];
				}
				else
				{
					width = tree.gridW;
					height = tree.gridH;
				}
                return spatialPrefixTree.ctx.MakeRectangle(xmin, xmin + width, ymin, ymin + height);
			}
		}//QuadCell

	}
}
