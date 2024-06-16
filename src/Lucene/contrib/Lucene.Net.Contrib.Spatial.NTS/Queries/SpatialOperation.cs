/* See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * Esri Inc. licenses this file to You under the Apache License, Version 2.0
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
using System.Globalization;
using System.Linq;
using Spatial4n.Exceptions;

namespace Lucene.Net.Spatial.Queries
{
	public class SpatialOperation
	{
		// Private registry
		private static readonly Dictionary<String, SpatialOperation> registry = new Dictionary<string, SpatialOperation>();
		private static readonly IList<SpatialOperation> list = new List<SpatialOperation>();

		// Geometry Operations

        /// <summary>
        /// Bounding box of the *indexed* shape.
        /// </summary>
		public static readonly SpatialOperation BBoxIntersects = new SpatialOperation("BBoxIntersects", true, false, false);
        
        /// <summary>
        /// Bounding box of the *indexed* shape.
        /// </summary>
		public static readonly SpatialOperation BBoxWithin = new SpatialOperation("BBoxWithin", true, false, false);

		public static readonly SpatialOperation Contains = new SpatialOperation("Contains", true, true, false);
		public static readonly SpatialOperation Intersects = new SpatialOperation("Intersects", true, false, false);
		public static readonly SpatialOperation IsEqualTo = new SpatialOperation("IsEqualTo", false, false, false);
		public static readonly SpatialOperation IsDisjointTo = new SpatialOperation("IsDisjointTo", false, false, false);
		public static readonly SpatialOperation IsWithin = new SpatialOperation("IsWithin", true, false, true);
		public static readonly SpatialOperation Overlaps = new SpatialOperation("Overlaps", true, false, true);

		// Member variables
		private readonly bool scoreIsMeaningful;
		private readonly bool sourceNeedsArea;
		private readonly bool targetNeedsArea;
		private readonly String name;

		protected SpatialOperation(String name, bool scoreIsMeaningful, bool sourceNeedsArea, bool targetNeedsArea)
		{
			this.name = name;
			this.scoreIsMeaningful = scoreIsMeaningful;
			this.sourceNeedsArea = sourceNeedsArea;
			this.targetNeedsArea = targetNeedsArea;
			var upperName = name.ToUpper(CultureInfo.CreateSpecificCulture("en-US"));
			registry[name] = this;
			registry[upperName] = this;
			list.Add(this);
		}

		public static SpatialOperation Get(String v)
		{
			SpatialOperation op;
			if (!registry.TryGetValue(v, out op) || op == null)
			{
				var upperV = v.ToUpper(CultureInfo.CreateSpecificCulture("en-US"));
				if (!registry.TryGetValue(upperV, out op) || op == null)
					throw new ArgumentException("Unknown Operation: " + v, "v");
			}
			return op;
		}

		public static IList<SpatialOperation> Values()
		{
			return list;
		}

		public static bool Is(SpatialOperation op, params SpatialOperation[] tst)
		{
			return tst.Any(t => op == t);
		}


		// ================================================= Getters / Setters =============================================

		public bool IsScoreIsMeaningful()
		{
			return scoreIsMeaningful;
		}

		public bool IsSourceNeedsArea()
		{
			return sourceNeedsArea;
		}

		public bool IsTargetNeedsArea()
		{
			return targetNeedsArea;
		}

		public String GetName()
		{
			return name;
		}

		public override String ToString()
		{
			return name;
		}

	}
}
