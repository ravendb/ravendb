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
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Search.Function;
using Lucene.Net.Spatial.Queries;
using Lucene.Net.Spatial.Util;
using Spatial4n.Context;
using Spatial4n.Shapes;

namespace Lucene.Net.Spatial.BBox
{
    public class BBoxStrategy : SpatialStrategy
    {
        public static String SUFFIX_MINX = "__minX";
        public static String SUFFIX_MAXX = "__maxX";
        public static String SUFFIX_MINY = "__minY";
        public static String SUFFIX_MAXY = "__maxY";
        public static String SUFFIX_XDL = "__xdl";

        /*
		 * The Bounding Box gets stored as four fields for x/y min/max and a flag
		 * that says if the box crosses the dateline (xdl).
		 */
        public readonly String field_bbox;
        public readonly String field_minX;
        public readonly String field_minY;
        public readonly String field_maxX;
        public readonly String field_maxY;
        public readonly String field_xdl; // crosses dateline

        public readonly double queryPower = 1.0;
        public readonly double targetPower = 1.0f;
        public int precisionStep = 8; // same as solr default

        public BBoxStrategy(SpatialContext ctx, String fieldNamePrefix)
            : base(ctx, fieldNamePrefix)
        {
            field_bbox = fieldNamePrefix;
            field_minX = fieldNamePrefix + SUFFIX_MINX;
            field_maxX = fieldNamePrefix + SUFFIX_MAXX;
            field_minY = fieldNamePrefix + SUFFIX_MINY;
            field_maxY = fieldNamePrefix + SUFFIX_MAXY;
            field_xdl = fieldNamePrefix + SUFFIX_XDL;
        }

        public void SetPrecisionStep(int p)
        {
            precisionStep = p;
            if (precisionStep <= 0 || precisionStep >= 64)
                precisionStep = int.MaxValue;
        }

        //---------------------------------
        // Indexing
        //---------------------------------

        public override AbstractField[] CreateIndexableFields(IShape shape)
        {
            var rect = shape as IRectangle;
            if (rect != null)
                return CreateIndexableFields(rect);
            throw new InvalidOperationException("Can only index Rectangle, not " + shape);
        }

        public AbstractField[] CreateIndexableFields(IRectangle bbox)
        {
            var fields = new AbstractField[5];
            fields[0] = DoubleField(field_minX, bbox.MinX);
            fields[1] = DoubleField(field_maxX, bbox.MaxX);
            fields[2] = DoubleField(field_minY, bbox.MinY);
            fields[3] = DoubleField(field_maxY, bbox.MaxY);
            fields[4] = new Field(field_xdl, bbox.CrossesDateLine ? "T" : "F", Field.Store.NO,
                                  Field.Index.NOT_ANALYZED_NO_NORMS) {OmitNorms = true, OmitTermFreqAndPositions = true};
            return fields;
        }

        private AbstractField DoubleField(string field, double value)
        {
            var f = new NumericField(field, precisionStep, Field.Store.NO, true)
                        {OmitNorms = true, OmitTermFreqAndPositions = true};
            f.SetDoubleValue(value);
            return f;
        }

        public override ValueSource MakeDistanceValueSource(IPoint queryPoint)
        {
            return new BBoxSimilarityValueSource(this, new DistanceSimilarity(this.GetSpatialContext(), queryPoint));
        }

        public ValueSource MakeBBoxAreaSimilarityValueSource(IRectangle queryBox)
        {
            return new BBoxSimilarityValueSource(
                this, new AreaSimilarity(queryBox, queryPower, targetPower));
        }

        public override ConstantScoreQuery MakeQuery(SpatialArgs args)
        {
            return new ConstantScoreQuery(new QueryWrapperFilter(MakeSpatialQuery(args)));
        }

        public Query MakeQueryWithValueSource(SpatialArgs args, ValueSource valueSource)
        {

            var bq = new BooleanQuery();
            var spatial = MakeFilter(args);
            bq.Add(new ConstantScoreQuery(spatial), Occur.MUST);

            // This part does the scoring
            Query spatialRankingQuery = new FunctionQuery(valueSource);
            bq.Add(spatialRankingQuery, Occur.MUST);
            return bq;
        }

        public override Filter MakeFilter(SpatialArgs args)
		{
            return new QueryWrapperFilter(MakeSpatialQuery(args));
		}

		private Query MakeSpatialQuery(SpatialArgs args)
		{
            var bbox = args.Shape as IRectangle;
            if (bbox == null)
                throw new InvalidOperationException("Can only query by Rectangle, not " + args.Shape);

			Query spatial = null;

			// Useful for understanding Relations:
			// http://edndoc.esri.com/arcsde/9.1/general_topics/understand_spatial_relations.htm
			SpatialOperation op = args.Operation;
			if (op == SpatialOperation.BBoxIntersects) spatial = MakeIntersects(bbox);
			else if (op == SpatialOperation.BBoxWithin) spatial = MakeWithin(bbox);
			else if (op == SpatialOperation.Contains) spatial = MakeContains(bbox);
			else if (op == SpatialOperation.Intersects) spatial = MakeIntersects(bbox);
			else if (op == SpatialOperation.IsEqualTo) spatial = MakeEquals(bbox);
			else if (op == SpatialOperation.IsDisjointTo) spatial = MakeDisjoint(bbox);
			else if (op == SpatialOperation.IsWithin) spatial = MakeWithin(bbox);
			else if (op == SpatialOperation.Overlaps) spatial = MakeIntersects(bbox);
			else
			{
				throw new UnsupportedSpatialOperation(op);
			}
			return spatial;
		}

		//-------------------------------------------------------------------------------
		//
		//-------------------------------------------------------------------------------

		/// <summary>
		/// Constructs a query to retrieve documents that fully contain the input envelope.
		/// </summary>
		/// <param name="bbox"></param>
		/// <returns>The spatial query</returns>
		protected Query MakeContains(IRectangle bbox)
		{

			// general case
			// docMinX <= queryExtent.GetMinX() AND docMinY <= queryExtent.GetMinY() AND docMaxX >= queryExtent.GetMaxX() AND docMaxY >= queryExtent.GetMaxY()

			// Y conditions
			// docMinY <= queryExtent.GetMinY() AND docMaxY >= queryExtent.GetMaxY()
			Query qMinY = NumericRangeQuery.NewDoubleRange(field_minY, precisionStep, null, bbox.MinY, false, true);
			Query qMaxY = NumericRangeQuery.NewDoubleRange(field_maxY, precisionStep, bbox.MaxY, null, true, false);
			Query yConditions = this.MakeQuery(new Query[] { qMinY, qMaxY }, Occur.MUST);

			// X conditions
			Query xConditions = null;

			// queries that do not cross the date line
			if (!bbox.CrossesDateLine)
			{

				// X Conditions for documents that do not cross the date line,
				// documents that contain the min X and max X of the query envelope,
				// docMinX <= queryExtent.GetMinX() AND docMaxX >= queryExtent.GetMaxX()
				Query qMinX = NumericRangeQuery.NewDoubleRange(field_minX, precisionStep, null, bbox.MinX, false, true);
				Query qMaxX = NumericRangeQuery.NewDoubleRange(field_maxX, precisionStep, bbox.MaxX, null, true, false);
				Query qMinMax = this.MakeQuery(new Query[] { qMinX, qMaxX }, Occur.MUST);
				Query qNonXDL = this.MakeXDL(false, qMinMax);

				// X Conditions for documents that cross the date line,
				// the left portion of the document contains the min X of the query
				// OR the right portion of the document contains the max X of the query,
				// docMinXLeft <= queryExtent.GetMinX() OR docMaxXRight >= queryExtent.GetMaxX()
				Query qXDLLeft = NumericRangeQuery.NewDoubleRange(field_minX, precisionStep, null, bbox.MinX, false, true);
				Query qXDLRight = NumericRangeQuery.NewDoubleRange(field_maxX, precisionStep, bbox.MaxX, null, true, false);
				Query qXDLLeftRight = this.MakeQuery(new Query[] { qXDLLeft, qXDLRight }, Occur.SHOULD);
				Query qXDL = this.MakeXDL(true, qXDLLeftRight);

				// apply the non-XDL and XDL conditions
				xConditions = this.MakeQuery(new Query[] { qNonXDL, qXDL }, Occur.SHOULD);

				// queries that cross the date line
			}
			else
			{

				// No need to search for documents that do not cross the date line

				// X Conditions for documents that cross the date line,
				// the left portion of the document contains the min X of the query
				// AND the right portion of the document contains the max X of the query,
				// docMinXLeft <= queryExtent.GetMinX() AND docMaxXRight >= queryExtent.GetMaxX()
				Query qXDLLeft = NumericRangeQuery.NewDoubleRange(field_minX, precisionStep, null, bbox.MinX, false, true);
				Query qXDLRight = NumericRangeQuery.NewDoubleRange(field_maxX, precisionStep, bbox.MaxX, null, true, false);
				Query qXDLLeftRight = this.MakeQuery(new Query[] { qXDLLeft, qXDLRight }, Occur.MUST);

				xConditions = this.MakeXDL(true, qXDLLeftRight);
			}

			// both X and Y conditions must occur
			return this.MakeQuery(new Query[] { xConditions, yConditions }, Occur.MUST);
		}

		/// <summary>
		/// Constructs a query to retrieve documents that are disjoint to the input envelope.
		/// </summary>
		/// <param name="bbox"></param>
		/// <returns>the spatial query</returns>
		Query MakeDisjoint(IRectangle bbox)
		{

			// general case
			// docMinX > queryExtent.GetMaxX() OR docMaxX < queryExtent.GetMinX() OR docMinY > queryExtent.GetMaxY() OR docMaxY < queryExtent.GetMinY()

			// Y conditions
			// docMinY > queryExtent.GetMaxY() OR docMaxY < queryExtent.GetMinY()
			Query qMinY = NumericRangeQuery.NewDoubleRange(field_minY, precisionStep, bbox.MaxY, null, false, false);
			Query qMaxY = NumericRangeQuery.NewDoubleRange(field_maxY, precisionStep, null, bbox.MinY, false, false);
			Query yConditions = this.MakeQuery(new Query[] { qMinY, qMaxY }, Occur.SHOULD);

			// X conditions
			Query xConditions = null;

			// queries that do not cross the date line
			if (!bbox.CrossesDateLine)
			{

				// X Conditions for documents that do not cross the date line,
				// docMinX > queryExtent.GetMaxX() OR docMaxX < queryExtent.GetMinX()
				Query qMinX = NumericRangeQuery.NewDoubleRange(field_minX, precisionStep, bbox.MaxX, null, false, false);
				Query qMaxX = NumericRangeQuery.NewDoubleRange(field_maxX, precisionStep, null, bbox.MinX, false, false);
				Query qMinMax = this.MakeQuery(new Query[] { qMinX, qMaxX }, Occur.SHOULD);
				Query qNonXDL = this.MakeXDL(false, qMinMax);

				// X Conditions for documents that cross the date line,
				// both the left and right portions of the document must be disjoint to the query
				// (docMinXLeft > queryExtent.GetMaxX() OR docMaxXLeft < queryExtent.GetMinX()) AND
				// (docMinXRight > queryExtent.GetMaxX() OR docMaxXRight < queryExtent.GetMinX())
				// where: docMaxXLeft = 180.0, docMinXRight = -180.0
				// (docMaxXLeft  < queryExtent.GetMinX()) equates to (180.0  < queryExtent.GetMinX()) and is ignored
				// (docMinXRight > queryExtent.GetMaxX()) equates to (-180.0 > queryExtent.GetMaxX()) and is ignored
				Query qMinXLeft = NumericRangeQuery.NewDoubleRange(field_minX, precisionStep, bbox.MaxX, null, false, false);
				Query qMaxXRight = NumericRangeQuery.NewDoubleRange(field_maxX, precisionStep, null, bbox.MinX, false, false);
				Query qLeftRight = this.MakeQuery(new Query[] { qMinXLeft, qMaxXRight }, Occur.MUST);
				Query qXDL = this.MakeXDL(true, qLeftRight);

				// apply the non-XDL and XDL conditions
				xConditions = this.MakeQuery(new Query[] { qNonXDL, qXDL }, Occur.SHOULD);

				// queries that cross the date line
			}
			else
			{

				// X Conditions for documents that do not cross the date line,
				// the document must be disjoint to both the left and right query portions
				// (docMinX > queryExtent.GetMaxX()Left OR docMaxX < queryExtent.GetMinX()) AND (docMinX > queryExtent.GetMaxX() OR docMaxX < queryExtent.GetMinX()Left)
				// where: queryExtent.GetMaxX()Left = 180.0, queryExtent.GetMinX()Left = -180.0
				Query qMinXLeft = NumericRangeQuery.NewDoubleRange(field_minX, precisionStep, 180.0, null, false, false);
				Query qMaxXLeft = NumericRangeQuery.NewDoubleRange(field_maxX, precisionStep, null, bbox.MinX, false, false);
				Query qMinXRight = NumericRangeQuery.NewDoubleRange(field_minX, precisionStep, bbox.MaxX, null, false, false);
				Query qMaxXRight = NumericRangeQuery.NewDoubleRange(field_maxX, precisionStep, null, -180.0, false, false);
				Query qLeft = this.MakeQuery(new Query[] { qMinXLeft, qMaxXLeft }, Occur.SHOULD);
				Query qRight = this.MakeQuery(new Query[] { qMinXRight, qMaxXRight }, Occur.SHOULD);
				Query qLeftRight = this.MakeQuery(new Query[] { qLeft, qRight }, Occur.MUST);

				// No need to search for documents that do not cross the date line

				xConditions = this.MakeXDL(false, qLeftRight);
			}

			// either X or Y conditions should occur
			return this.MakeQuery(new Query[] { xConditions, yConditions }, Occur.SHOULD);
		}

		/*
		 * Constructs a query to retrieve documents that equal the input envelope.
		 *
		 * @return the spatial query
		 */
		public Query MakeEquals(IRectangle bbox)
		{

			// docMinX = queryExtent.GetMinX() AND docMinY = queryExtent.GetMinY() AND docMaxX = queryExtent.GetMaxX() AND docMaxY = queryExtent.GetMaxY()
			Query qMinX = NumericRangeQuery.NewDoubleRange(field_minX, precisionStep, bbox.MinX, bbox.MinX, true, true);
			Query qMinY = NumericRangeQuery.NewDoubleRange(field_minY, precisionStep, bbox.MinY, bbox.MinY, true, true);
			Query qMaxX = NumericRangeQuery.NewDoubleRange(field_maxX, precisionStep, bbox.MaxX, bbox.MaxX, true, true);
			Query qMaxY = NumericRangeQuery.NewDoubleRange(field_maxY, precisionStep, bbox.MaxY, bbox.MaxY, true, true);
			
			var bq = new BooleanQuery
			         	{
			         		{qMinX, Occur.MUST},
			         		{qMinY, Occur.MUST},
			         		{qMaxX, Occur.MUST},
			         		{qMaxY, Occur.MUST}
			         	};
			return bq;
		}

		/// <summary>
		/// Constructs a query to retrieve documents that intersect the input envelope.
		/// </summary>
		/// <param name="bbox"></param>
		/// <returns>the spatial query</returns>
		Query MakeIntersects(IRectangle bbox)
		{

			// the original intersects query does not work for envelopes that cross the date line,
			// switch to a NOT Disjoint query

			// MUST_NOT causes a problem when it's the only clause type within a BooleanQuery,
			// to get round it we add all documents as a SHOULD

			// there must be an envelope, it must not be disjoint
			Query qDisjoint = MakeDisjoint(bbox);
			Query qIsNonXDL = this.MakeXDL(false);
			Query qIsXDL = this.MakeXDL(true);
			Query qHasEnv = this.MakeQuery(new Query[] { qIsNonXDL, qIsXDL }, Occur.SHOULD);
			var qNotDisjoint = new BooleanQuery {{qHasEnv, Occur.MUST}, {qDisjoint, Occur.MUST_NOT}};

			//Query qDisjoint = makeDisjoint();
			//BooleanQuery qNotDisjoint = new BooleanQuery();
			//qNotDisjoint.add(new MatchAllDocsQuery(),BooleanClause.Occur.SHOULD);
			//qNotDisjoint.add(qDisjoint,BooleanClause.Occur.MUST_NOT);
			return qNotDisjoint;
		}

		/*
		 * Makes a boolean query based upon a collection of queries and a logical operator.
		 *
		 * @param queries the query collection
		 * @param occur the logical operator
		 * @return the query
		 */
		BooleanQuery MakeQuery(Query[] queries, Occur occur)
		{
			var bq = new BooleanQuery();
			foreach (Query query in queries)
			{
				bq.Add(query, occur);
			}
			return bq;
		}

		/*
		 * Constructs a query to retrieve documents are fully within the input envelope.
		 *
		 * @return the spatial query
		 */
		Query MakeWithin(IRectangle bbox)
		{

			// general case
			// docMinX >= queryExtent.GetMinX() AND docMinY >= queryExtent.GetMinY() AND docMaxX <= queryExtent.GetMaxX() AND docMaxY <= queryExtent.GetMaxY()

			// Y conditions
			// docMinY >= queryExtent.GetMinY() AND docMaxY <= queryExtent.GetMaxY()
			Query qMinY = NumericRangeQuery.NewDoubleRange(field_minY, precisionStep, bbox.MinY, null, true, false);
			Query qMaxY = NumericRangeQuery.NewDoubleRange(field_maxY, precisionStep, null, bbox.MaxY, false, true);
			Query yConditions = this.MakeQuery(new Query[] { qMinY, qMaxY }, Occur.MUST);

			// X conditions
			Query xConditions = null;

			// X Conditions for documents that cross the date line,
			// the left portion of the document must be within the left portion of the query,
			// AND the right portion of the document must be within the right portion of the query
			// docMinXLeft >= queryExtent.GetMinX() AND docMaxXLeft <= 180.0
			// AND docMinXRight >= -180.0 AND docMaxXRight <= queryExtent.GetMaxX()
			Query qXDLLeft = NumericRangeQuery.NewDoubleRange(field_minX, precisionStep, bbox.MinX, null, true, false);
			Query qXDLRight = NumericRangeQuery.NewDoubleRange(field_maxX, precisionStep, null, bbox.MaxX, false, true);
			Query qXDLLeftRight = this.MakeQuery(new Query[] { qXDLLeft, qXDLRight }, Occur.MUST);
			Query qXDL = this.MakeXDL(true, qXDLLeftRight);

			// queries that do not cross the date line
			if (!bbox.CrossesDateLine)
			{

				// X Conditions for documents that do not cross the date line,
				// docMinX >= queryExtent.GetMinX() AND docMaxX <= queryExtent.GetMaxX()
				Query qMinX = NumericRangeQuery.NewDoubleRange(field_minX, precisionStep, bbox.MinX, null, true, false);
				Query qMaxX = NumericRangeQuery.NewDoubleRange(field_maxX, precisionStep, null, bbox.MaxX, false, true);
				Query qMinMax = this.MakeQuery(new Query[] { qMinX, qMaxX }, Occur.MUST);
				Query qNonXDL = this.MakeXDL(false, qMinMax);

				// apply the non-XDL or XDL X conditions
				if ((bbox.MinX <= -180.0) && bbox.MaxX >= 180.0)
				{
					xConditions = this.MakeQuery(new Query[] { qNonXDL, qXDL }, Occur.SHOULD);
				}
				else
				{
					xConditions = qNonXDL;
				}

				// queries that cross the date line
			}
			else
			{

				// X Conditions for documents that do not cross the date line

				// the document should be within the left portion of the query
				// docMinX >= queryExtent.GetMinX() AND docMaxX <= 180.0
				Query qMinXLeft = NumericRangeQuery.NewDoubleRange(field_minX, precisionStep, bbox.MinX, null, true, false);
				Query qMaxXLeft = NumericRangeQuery.NewDoubleRange(field_maxX, precisionStep, null, 180.0, false, true);
				Query qLeft = this.MakeQuery(new Query[] { qMinXLeft, qMaxXLeft }, Occur.MUST);

				// the document should be within the right portion of the query
				// docMinX >= -180.0 AND docMaxX <= queryExtent.GetMaxX()
				Query qMinXRight = NumericRangeQuery.NewDoubleRange(field_minX, precisionStep, -180.0, null, true, false);
				Query qMaxXRight = NumericRangeQuery.NewDoubleRange(field_maxX, precisionStep, null, bbox.MaxX, false, true);
				Query qRight = this.MakeQuery(new Query[] { qMinXRight, qMaxXRight }, Occur.MUST);

				// either left or right conditions should occur,
				// apply the left and right conditions to documents that do not cross the date line
				Query qLeftRight = this.MakeQuery(new Query[] { qLeft, qRight }, Occur.SHOULD);
				Query qNonXDL = this.MakeXDL(false, qLeftRight);

				// apply the non-XDL and XDL conditions
				xConditions = this.MakeQuery(new Query[] { qNonXDL, qXDL }, Occur.SHOULD);
			}

			// both X and Y conditions must occur
			return this.MakeQuery(new Query[] { xConditions, yConditions }, Occur.MUST);
		}

		/*
		 * Constructs a query to retrieve documents that do or do not cross the date line.
		 *
		 *
		 * @param crossedDateLine <code>true</true> for documents that cross the date line
		 * @return the query
		 */
		public Query MakeXDL(bool crossedDateLine)
		{
			// The 'T' and 'F' values match solr fields
			return new TermQuery(new Term(field_xdl, crossedDateLine ? "T" : "F"));
		}

		/*
		 * Constructs a query to retrieve documents that do or do not cross the date line
		 * and match the supplied spatial query.
		 *
		 * @param crossedDateLine <code>true</true> for documents that cross the date line
		 * @param query the spatial query
		 * @return the query
		 */
		public Query MakeXDL(bool crossedDateLine, Query query)
		{
			var bq = new BooleanQuery
			         	{{this.MakeXDL(crossedDateLine), Occur.MUST}, {query, Occur.MUST}};
			return bq;
		}
	}
}
