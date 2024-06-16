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
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Spatial4n.Context;
using Spatial4n.Shapes;

namespace Lucene.Net.Spatial.Prefix.Tree
{
	/// <summary>
    /// A spatial Prefix Tree, or Trie, which decomposes shapes into prefixed strings at variable lengths corresponding to
	/// variable precision.  Each string corresponds to a spatial region.
	/// 
	/// Implementations of this class should be thread-safe and immutable once initialized. 
	/// </summary>
	public abstract class SpatialPrefixTree
	{
		protected readonly int maxLevels;
		internal readonly SpatialContext ctx;// it's internal to allow Node to access it

		protected SpatialPrefixTree(SpatialContext ctx, int maxLevels)
		{
			Debug.Assert(maxLevels > 0);
			this.ctx = ctx;
			this.maxLevels = maxLevels;
		}

		public SpatialContext GetSpatialContext()
		{
			return ctx;
		}

		public int GetMaxLevels()
		{
			return maxLevels;
		}

		public override String ToString()
		{
			return GetType().Name + "(maxLevels:" + maxLevels + ",ctx:" + ctx + ")";
		}

	    /// <summary>
	    /// Returns the level of the largest grid in which its longest side is less
	    /// than or equal to the provided distance (in degrees). Consequently {@link
	    /// dist} acts as an error epsilon declaring the amount of detail needed in the
	    /// grid, such that you can get a grid with just the right amount of
	    /// precision.
	    /// </summary>
        /// <param name="dist">>= 0</param>
        /// <returns>level [1 to maxLevels]</returns>
		public abstract int GetLevelForDistance(double dist);

		//TODO double getDistanceForLevel(int level)

		//[NotSerialized]
		private Node worldNode;//cached

		/*
		 * Returns the level 0 cell which encompasses all spatial data. Equivalent to {@link #getNode(String)} with "".
		 * This cell is threadsafe, just like a spatial prefix grid is, although cells aren't
		 * generally threadsafe.
		 * TODO rename to getTopCell or is this fine?
		 */
		public Node GetWorldNode()
		{
			if (worldNode == null)
			{
				worldNode = GetNode("");
			}
			return worldNode;
		}

		/*
		 * The cell for the specified token. The empty string should be equal to {@link #getWorldNode()}.
		 * Precondition: Never called when token length > maxLevel.
		 */
		public abstract Node GetNode(String token);

		public abstract Node GetNode(byte[] bytes, int offset, int len);

		//public Node GetNode(byte[] bytes, int offset, int len, Node target)
		//{
		//    if (target == null)
		//    {
		//        return GetNode(bytes, offset, len);
		//    }

		//    target.Reset(bytes, offset, len);
		//    return target;
		//}

		public Node GetNode(string token, Node target)
		{
			if (target == null)
			{
				return GetNode(token);
			}

			target.Reset(token);
			return target;
		}

		protected virtual Node GetNode(IPoint p, int level)
		{
			return GetNodes(p, level, false).ElementAt(0);
		}

		/*
		 * Gets the intersecting & including cells for the specified shape, without exceeding detail level.
		 * The result is a set of cells (no dups), sorted. Unmodifiable.
		 * <p/>
		 * This implementation checks if shape is a Point and if so uses an implementation that
		 * recursively calls {@link Node#getSubCell(com.spatial4j.core.shape.Point)}. Cell subclasses
		 * ideally implement that method with a quick implementation, otherwise, subclasses should
		 * override this method to invoke {@link #getNodesAltPoint(com.spatial4j.core.shape.Point, int, boolean)}.
		 * TODO consider another approach returning an iterator -- won't build up all cells in memory.
		 */
		public virtual IList<Node> GetNodes(IShape shape, int detailLevel, bool inclParents)
		{
			if (detailLevel > maxLevels)
			{
				throw new ArgumentException("detailLevel > maxLevels", "detailLevel");
			}

			List<Node> cells;
			if (shape is IPoint)
			{
				//optimized point algorithm
				int initialCapacity = inclParents ? 1 + detailLevel : 1;
				cells = new List<Node>(initialCapacity);
				RecursiveGetNodes(GetWorldNode(), (IPoint)shape, detailLevel, true, cells);
				Debug.Assert(cells.Count == initialCapacity);
			}
			else
			{
				cells = new List<Node>(inclParents ? 1024 : 512);
				RecursiveGetNodes(GetWorldNode(), shape, detailLevel, inclParents, cells);
			}
			if (inclParents)
			{
				Debug.Assert(cells[0].GetLevel() == 0);
				cells.RemoveAt(0);//remove getWorldNode()
			}
			return cells;
		}

		private void RecursiveGetNodes(Node node, IShape shape, int detailLevel, bool inclParents, IList<Node> result)
		{
			if (node.IsLeaf())
			{//cell is within shape
				result.Add(node);
				return;
			}

			var subCells = node.GetSubCells(shape);
			if (node.GetLevel() == detailLevel - 1)
			{
				if (subCells.Count < node.GetSubCellsSize())
				{
					if (inclParents)
						result.Add(node);
					foreach (var subCell in subCells)
					{
						subCell.SetLeaf();
						result.Add(subCell);
					}
				}
				else
				{//a bottom level (i.e. detail level) optimization where all boxes intersect, so use parent cell.
					node.SetLeaf();
					result.Add(node);
				}
			}
			else
			{
				if (inclParents)
				{
					result.Add(node);
				}
				foreach (var subCell in subCells)
				{
					RecursiveGetNodes(subCell, shape, detailLevel, inclParents, result);//tail call
				}
			}
		}

		private void RecursiveGetNodes(Node node, IPoint point, int detailLevel, bool inclParents, IList<Node> result)
		{
			if (inclParents)
			{
				result.Add(node);
			}
			Node pCell = node.GetSubCell(point);
			if (node.GetLevel() == detailLevel - 1)
			{
				pCell.SetLeaf();
				result.Add(pCell);
			}
			else
			{
				RecursiveGetNodes(pCell, point, detailLevel, inclParents, result);//tail call
			}
		}

		/*
		 * Subclasses might override {@link #getNodes(com.spatial4j.core.shape.Shape, int, boolean)}
		 * and check if the argument is a shape and if so, delegate
		 * to this implementation, which calls {@link #getNode(com.spatial4j.core.shape.Point, int)} and
		 * then calls {@link #getNode(String)} repeatedly if inclParents is true.
		 */
		protected virtual IList<Node> GetNodesAltPoint(IPoint p, int detailLevel, bool inclParents)
		{
			Node cell = GetNode(p, detailLevel);
			if (!inclParents)
			{
#if !NET35
				return new ReadOnlyCollectionBuilder<Node>(new[] { cell }).ToReadOnlyCollection();
#else
                return new List<Node>(new[] { cell }).AsReadOnly();
#endif
			}

			String endToken = cell.GetTokenString();
			Debug.Assert(endToken.Length == detailLevel);
			var cells = new List<Node>(detailLevel);
			for (int i = 1; i < detailLevel; i++)
			{
				cells.Add(GetNode(endToken.Substring(0, i)));
			}
			cells.Add(cell);
			return cells;
		}

		/*
		 * Will add the trailing leaf byte for leaves. This isn't particularly efficient.
		 */
		public static List<String> NodesToTokenStrings(Collection<Node> nodes)
		{
			var tokens = new List<String>((nodes.Count));
			foreach (Node node in nodes)
			{
				String token = node.GetTokenString();
				if (node.IsLeaf())
				{
					tokens.Add(token + (char)Node.LEAF_BYTE);
				}
				else
				{
					tokens.Add(token);
				}
			}
			return tokens;
		}

	}
}
