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
using System.Runtime.CompilerServices;
using Spatial4n.Shapes;

namespace Lucene.Net.Spatial.Prefix.Tree
{
	public abstract class Node : IComparable<Node>
	{
		public static byte LEAF_BYTE = (byte)'+';//NOTE: must sort before letters & numbers

		// /*
		//Holds a byte[] and/or String representation of the cell. Both are lazy constructed from the other.
		//Neither contains the trailing leaf byte.
		// */
		//private byte[] bytes;
		//private int b_off;
		//private int b_len;

		private String token;//this is the only part of equality

		protected SpatialRelation shapeRel;//set in getSubCells(filter), and via setLeaf().
		protected readonly SpatialPrefixTree spatialPrefixTree;

		protected Node(SpatialPrefixTree spatialPrefixTree, String token)
		{
			this.spatialPrefixTree = spatialPrefixTree;
			SetToken(token);

			if (GetLevel() == 0)
				GetShape();//ensure any lazy instantiation completes to make this threadsafe
		}

		private void SetToken(String theToken)
		{
			this.token = theToken;
			if (token.Length > 0 && token[token.Length - 1] == (char)LEAF_BYTE)
			{
				this.token = token.Substring(0, token.Length - 1);
				SetLeaf();
			}
		}

		public virtual void Reset(string newToken)
		{
			Debug.Assert(GetLevel() != 0);
			shapeRel = SpatialRelation.None;
			SetToken(newToken);
			b_fixLeaf();
		}

		private void b_fixLeaf()
		{
			if (GetLevel() == spatialPrefixTree.GetMaxLevels())
			{
				SetLeaf();
			}
		}

		public SpatialRelation GetShapeRel()
		{
			return shapeRel;
		}

		public bool IsLeaf()
		{
			return shapeRel == SpatialRelation.Within;
		}

		public void SetLeaf()
		{
			Debug.Assert(GetLevel() != 0);
			shapeRel = SpatialRelation.Within;
		}

		/*
		 * Note: doesn't contain a trailing leaf byte.
		 */
		public String GetTokenString()
		{
			if (token == null)
				throw new InvalidOperationException("Somehow we got a null token");
			return token;
		}

		///// <summary>
		///// Note: doesn't contain a trailing leaf byte.
		///// </summary>
		///// <returns></returns>
		//public byte[] GetTokenBytes()
		//{
		//    if (bytes != null)
		//    {
		//        if (b_off != 0 || b_len != bytes.Length)
		//        {
		//            throw new IllegalStateException("Not supported if byte[] needs to be recreated.");
		//        }
		//    }
		//    else
		//    {
		//        bytes = token.GetBytes(SpatialPrefixTree.UTF8);
		//        b_off = 0;
		//        b_len = bytes.Length;
		//    }
		//    return bytes;
		//}

		public int GetLevel()
		{
			return token.Length;
			//return token != null ? token.Length : b_len;
		}

		//TODO add getParent() and update some algorithms to use this?
		//public Cell getParent();

		/*
		 * Like {@link #getSubCells()} but with the results filtered by a shape. If that shape is a {@link com.spatial4j.core.shape.Point} then it
		 * must call {@link #getSubCell(com.spatial4j.core.shape.Point)};
		 * Precondition: Never called when getLevel() == maxLevel.
		 *
		 * @param shapeFilter an optional filter for the returned cells.
		 * @return A set of cells (no dups), sorted. Not Modifiable.
		 */
		public IList<Node> GetSubCells(IShape shapeFilter)
		{
			//Note: Higher-performing subclasses might override to consider the shape filter to generate fewer cells.
			var point = shapeFilter as IPoint;
			if (point != null)
			{
#if !NET35
				return new ReadOnlyCollectionBuilder<Node>(new[] {GetSubCell(point)}).ToReadOnlyCollection();
#else
                return new List<Node>(new[]{GetSubCell(point)}).AsReadOnly();
#endif

			}

			var cells = GetSubCells();
			if (shapeFilter == null)
			{
				return cells;
			}
			var copy = new List<Node>(cells.Count);//copy since cells contractually isn't modifiable
			foreach (var cell in cells)
			{
                SpatialRelation rel = cell.GetShape().Relate(shapeFilter);
				if (rel == SpatialRelation.Disjoint)
					continue;
				cell.shapeRel = rel;
				copy.Add(cell);
			}
			cells = copy;
			return cells;
		}

		/*
		 * Performant implementations are expected to implement this efficiently by considering the current
		 * cell's boundary.
		 * Precondition: Never called when getLevel() == maxLevel.
		 * Precondition: this.getShape().relate(p) != DISJOINT.
		 */
		public abstract Node GetSubCell(IPoint p);

		//TODO Cell getSubCell(byte b)

		/*
		 * Gets the cells at the next grid cell level that cover this cell.
		 * Precondition: Never called when getLevel() == maxLevel.
		 *
		 * @return A set of cells (no dups), sorted. Not Modifiable.
		 */
		public abstract IList<Node> GetSubCells();

		/*
		 * {@link #getSubCells()}.size() -- usually a constant. Should be >=2
		 */
		public abstract int GetSubCellsSize();

		public abstract IShape GetShape();

		public virtual IPoint GetCenter()
		{
			return GetShape().Center;
		}


		public int CompareTo(Node o)
		{
			return System.String.CompareOrdinal(GetTokenString(), o.GetTokenString());
		}

		public override bool Equals(object obj)
		{
			return !(obj == null || !(obj is Node)) && GetTokenString().Equals(((Node) obj).GetTokenString());
		}

		public override int GetHashCode()
		{
			return GetTokenString().GetHashCode();
		}

		public override string ToString()
		{
			return GetTokenString() + (IsLeaf() ? new string(new[] {(char) LEAF_BYTE}) : string.Empty);
		}
	}
}
