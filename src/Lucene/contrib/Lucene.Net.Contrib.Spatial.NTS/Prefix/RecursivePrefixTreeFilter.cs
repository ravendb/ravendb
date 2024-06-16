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
using Lucene.Net.Search;
using Lucene.Net.Spatial.Prefix.Tree;
using Lucene.Net.Spatial.Util;
using Lucene.Net.Store;
using Lucene.Net.Util;
using Spatial4n.Shapes;

namespace Lucene.Net.Spatial.Prefix
{
	/// <summary>
	/// Performs a spatial intersection filter against a field indexed with {@link SpatialPrefixTree}, a Trie.
	/// SPT yields terms (grids) at length 1 and at greater lengths corresponding to greater precisions.
	/// This filter recursively traverses each grid length and uses methods on {@link Shape} to efficiently know
	/// that all points at a prefix fit in the shape or not to either short-circuit unnecessary traversals or to efficiently
	/// load all enclosed points.
	/// </summary>
	public class RecursivePrefixTreeFilter : Filter
	{
		/* TODOs for future:

Can a polygon query shape be optimized / made-simpler at recursive depths (e.g. intersection of shape + cell box)

RE "scan" threshold:
// IF configured to do so, we could use term.freq() as an estimate on the number of places at this depth.  OR, perhaps
//  make estimates based on the total known term count at this level?
if (!scan) {
  //Make some estimations on how many points there are at this level and how few there would need to be to set
  // !scan to false.
  long termsThreshold = (long) estimateNumberIndexedTerms(cell.length(),queryShape.getDocFreqExpenseThreshold(cell));
  long thisOrd = termsEnum.ord();
  scan = (termsEnum.seek(thisOrd+termsThreshold+1) == TermsEnum.SeekStatus.END
		  || !cell.contains(termsEnum.term()));
  termsEnum.seek(thisOrd);//return to last position
}

*/

		private readonly String fieldName;
		private readonly SpatialPrefixTree grid;
		private readonly IShape queryShape;
		private readonly int prefixGridScanLevel;//at least one less than grid.getMaxLevels()
		private readonly int detailLevel;

		public RecursivePrefixTreeFilter(String fieldName, SpatialPrefixTree grid, IShape queryShape, int prefixGridScanLevel,
							 int detailLevel)
		{
			this.fieldName = fieldName;
			this.grid = grid;
			this.queryShape = queryShape;
			this.prefixGridScanLevel = Math.Max(1, Math.Min(prefixGridScanLevel, grid.GetMaxLevels() - 1));
			this.detailLevel = detailLevel;
			Debug.Assert(detailLevel <= grid.GetMaxLevels());
		}

		public override DocIdSet GetDocIdSet(Index.IndexReader reader /*, Bits acceptDocs*/, IState state)
		{
			var bits = new OpenBitSet(reader.MaxDoc);
			var terms = new TermsEnumCompatibility(reader, fieldName, state);
			var term = terms.Next(state);
			if (term == null)
				return null;
			Node scanCell = null;

			//cells is treated like a stack. LinkedList conveniently has bulk add to beginning. It's in sorted order so that we
			//  always advance forward through the termsEnum index.
			var cells = new LinkedList<Node>(
				grid.GetWorldNode().GetSubCells(queryShape));

			//This is a recursive algorithm that starts with one or more "big" cells, and then recursively dives down into the
			// first such cell that intersects with the query shape.  It's a depth first traversal because we don't move onto
			// the next big cell (breadth) until we're completely done considering all smaller cells beneath it. For a given
			// cell, if it's *within* the query shape then we can conveniently short-circuit the depth traversal and
			// grab all documents assigned to this cell/term.  For an intersection of the cell and query shape, we either
			// recursively step down another grid level or we decide heuristically (via prefixGridScanLevel) that there aren't
			// that many points, and so we scan through all terms within this cell (i.e. the term starts with the cell's term),
			// seeing which ones are within the query shape.
			while (cells.Count > 0)
			{
				Node cell = cells.First.Value; cells.RemoveFirst();
				var cellTerm = cell.GetTokenString();
				var seekStat = terms.Seek(cellTerm, state);
				if (seekStat == TermsEnumCompatibility.SeekStatus.END)
					break;
				if (seekStat == TermsEnumCompatibility.SeekStatus.NOT_FOUND)
					continue;
				if (cell.GetLevel() == detailLevel || cell.IsLeaf())
				{
					terms.Docs(bits, state);
				}
				else
				{//any other intersection
					//If the next indexed term is the leaf marker, then add all of them
					var nextCellTerm = terms.Next(state);
					Debug.Assert(nextCellTerm.Text.StartsWith(cellTerm));
					scanCell = grid.GetNode(nextCellTerm.Text, scanCell);
					if (scanCell.IsLeaf())
					{
						terms.Docs(bits, state);
						term = terms.Next(state);//move pointer to avoid potential redundant addDocs() below
					}

					//Decide whether to continue to divide & conquer, or whether it's time to scan through terms beneath this cell.
					// Scanning is a performance optimization trade-off.
					bool scan = cell.GetLevel() >= prefixGridScanLevel;//simple heuristic

					if (!scan)
					{
						//Divide & conquer
						var lst = cell.GetSubCells(queryShape);
						for (var i = lst.Count - 1; i >= 0; i--) //add to beginning
						{
							cells.AddFirst(lst[i]);
						}
					}
					else
					{
						//Scan through all terms within this cell to see if they are within the queryShape. No seek()s.
						for (var t = terms.Term(); t != null && t.Text.StartsWith(cellTerm); t = terms.Next(state))
						{
							scanCell = grid.GetNode(t.Text, scanCell);
							int termLevel = scanCell.GetLevel();
							if (termLevel > detailLevel)
								continue;
							if (termLevel == detailLevel || scanCell.IsLeaf())
							{
								IShape cShape;
								if (termLevel == grid.GetMaxLevels() && queryShape.HasArea)
								//TODO should put more thought into implications of box vs point
									cShape = scanCell.GetCenter();
								else
									cShape = scanCell.GetShape();
                                if (queryShape.Relate(cShape) == SpatialRelation.Disjoint)
									continue;

								terms.Docs(bits, state);
							}
						}//term loop
					}
				}
			}//cell loop

			return bits;
		}

		public override string ToString()
		{
			return "GeoFilter{fieldName='" + fieldName + '\'' + ", shape=" + queryShape + '}';
		}

		public override bool Equals(object o)
		{
			if (this == o) return true;
			var that = o as RecursivePrefixTreeFilter;

			if (that == null) return false;

			if (!fieldName.Equals(that.fieldName)) return false;
			//note that we don't need to look at grid since for the same field it should be the same
			if (prefixGridScanLevel != that.prefixGridScanLevel) return false;
			if (detailLevel != that.detailLevel) return false;
			if (!queryShape.Equals(that.queryShape)) return false;

			return true;
		}

		public override int GetHashCode()
		{
			int result = fieldName.GetHashCode();
			result = 31 * result + queryShape.GetHashCode();
			result = 31 * result + detailLevel;
			return result;
		}
	}
}
