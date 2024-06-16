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
using Lucene.Net.Documents;
using Lucene.Net.Search;
using Lucene.Net.Search.Function;
using Lucene.Net.Spatial.Queries;
using Lucene.Net.Spatial.Util;
using Spatial4n.Context;
using Spatial4n.Shapes;

namespace Lucene.Net.Spatial
{
	/// <summary>
	/// The SpatialStrategy encapsulates an approach to indexing and searching based on shapes.
	/// <p/>
	/// Note that a SpatialStrategy is not involved with the Lucene stored field values of shapes, which is
	/// immaterial to indexing and search.
	/// <p/>
	/// Thread-safe.
	/// </summary>
	public abstract class SpatialStrategy
	{
		protected readonly SpatialContext ctx;
		protected readonly string fieldName;

	    /// <summary>
	    /// Constructs the spatial strategy with its mandatory arguments.
	    /// </summary>
	    /// <param name="ctx"></param>
	    /// <param name="fieldName"> </param>
	    protected SpatialStrategy(SpatialContext ctx, string fieldName)
		{
			if (ctx == null)
				throw new ArgumentException("ctx is required", "ctx");
			this.ctx = ctx;
			if (string.IsNullOrEmpty(fieldName))
				throw new ArgumentException("fieldName is required", "fieldName");
			this.fieldName = fieldName;
		}

		public SpatialContext GetSpatialContext()
		{
			return ctx;
		}

		/// <summary>
		/// The name of the field or the prefix of them if there are multiple
		/// fields needed internally.
		/// </summary>
		/// <returns></returns>
		public String GetFieldName()
		{
			return fieldName;
		}

		/// <summary>
		/// Returns the IndexableField(s) from the <c>shape</c> that are to be
		/// added to the {@link org.apache.lucene.document.Document}.  These fields
		/// are expected to be marked as indexed and not stored.
		/// <p/>
		/// Note: If you want to <i>store</i> the shape as a string for retrieval in search
		/// results, you could add it like this:
		/// <pre>document.add(new StoredField(fieldName,ctx.toString(shape)));</pre>
		/// The particular string representation used doesn't matter to the Strategy since it
		/// doesn't use it.
		/// </summary>
		/// <param name="shape"></param>
		/// <returns>Not null nor will it have null elements.</returns>
		public abstract AbstractField[] CreateIndexableFields(IShape shape);

		public AbstractField CreateStoredField(IShape shape)
		{
			return new Field(GetFieldName(), ctx.ToString(shape), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);
		}

		/// <summary>
		/// Make a ValueSource returning the distance between the center of the
		/// indexed shape and {@code queryPoint}.  If there are multiple indexed shapes
		/// then the closest one is chosen.
		/// </summary>
		public abstract ValueSource MakeDistanceValueSource(IPoint queryPoint);

	    /// <summary>
	    /// Make a (ConstantScore) Query based principally on {@link org.apache.lucene.spatial.query.SpatialOperation}
	    /// and {@link Shape} from the supplied {@code args}.
	    /// The default implementation is
	    /// <pre>return new ConstantScoreQuery(makeFilter(args));</pre>
	    /// </summary>
	    /// <param name="args"></param>
	    /// <returns></returns>
	    public virtual ConstantScoreQuery MakeQuery(SpatialArgs args)
		{
            return new ConstantScoreQuery(MakeFilter(args));
		}

		/// <summary>
		/// Make a Filter based principally on {@link org.apache.lucene.spatial.query.SpatialOperation}
		/// and {@link Shape} from the supplied {@code args}.
		/// <p />
		/// If a subclasses implements
		/// {@link #makeQuery(org.apache.lucene.spatial.query.SpatialArgs)}
		/// then this method could be simply:
		/// <pre>return new QueryWrapperFilter(makeQuery(args).getQuery());</pre>
		/// </summary>
		/// <param name="args"></param>
		/// <returns></returns>
		public abstract Filter MakeFilter(SpatialArgs args);

        /// <summary>
        /// Returns a ValueSource with values ranging from 1 to 0, depending inversely
        /// on the distance from {@link #makeDistanceValueSource(com.spatial4j.core.shape.Point)}.
        /// The formula is <c>c/(d + c)</c> where 'd' is the distance and 'c' is
        /// one tenth the distance to the farthest edge from the center. Thus the
        /// scores will be 1 for indexed points at the center of the query shape and as
        /// low as ~0.1 at its furthest edges.
        /// </summary>
        /// <param name="queryShape"></param>
        /// <returns></returns>
        public ValueSource MakeRecipDistanceValueSource(IShape queryShape)
        {
            IRectangle bbox = queryShape.BoundingBox;
            double diagonalDist = ctx.DistanceCalculator.Distance(
                ctx.MakePoint(bbox.MinX, bbox.MinY), bbox.MaxX, bbox.MaxY);
            double distToEdge = diagonalDist*0.5;
            float c = (float) distToEdge*0.1f; //one tenth
            return new ReciprocalFloatFunction(MakeDistanceValueSource(queryShape.Center), 1f, c, c);
        }

	    public override string ToString()
		{
			return GetType().Name + " field:" + fieldName + " ctx=" + ctx;
		}
	}
}
