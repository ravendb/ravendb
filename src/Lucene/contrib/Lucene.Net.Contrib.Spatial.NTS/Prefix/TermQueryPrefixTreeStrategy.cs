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
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Spatial.Prefix.Tree;
using Lucene.Net.Spatial.Queries;
using Lucene.Net.Spatial.Util;
using Spatial4n.Shapes;

namespace Lucene.Net.Spatial.Prefix
{
	/// <summary>
	/// A basic implementation using a large {@link TermsFilter} of all the nodes from
	/// {@link SpatialPrefixTree#getNodes(com.spatial4j.core.shape.Shape, int, boolean)}.
	/// </summary>
	public class TermQueryPrefixTreeStrategy : PrefixTreeStrategy
	{
		public TermQueryPrefixTreeStrategy(SpatialPrefixTree grid, string fieldName)
			: base(grid, fieldName)
		{
		}

		public override Filter MakeFilter(SpatialArgs args)
		{
			SpatialOperation op = args.Operation;
            if (op != SpatialOperation.Intersects)
				throw new UnsupportedSpatialOperation(op);

			IShape shape = args.Shape;
            int detailLevel = grid.GetLevelForDistance(args.ResolveDistErr(ctx, distErrPct));
			var cells = grid.GetNodes(shape, detailLevel, false);
			var filter = new TermsFilter();
			foreach (Node cell in cells)
			{
				filter.AddTerm(new Term(GetFieldName(), cell.GetTokenString()));
			}
			return filter;
		}
	}
}
