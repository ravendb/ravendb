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
using Lucene.Net.Search;
using Lucene.Net.Search.Function;
using Lucene.Net.Spatial.Util;
using Lucene.Net.Store;
using Spatial4n.Shapes;

namespace Lucene.Net.Spatial.BBox
{
	public class BBoxSimilarityValueSource : ValueSource
	{
		private readonly BBoxStrategy strategy;
		private readonly BBoxSimilarity similarity;

		public BBoxSimilarityValueSource(BBoxStrategy strategy, BBoxSimilarity similarity)
		{
			this.strategy = strategy;
			this.similarity = similarity;
		}

		private class BBoxSimilarityValueSourceDocValues : DocValues
		{
			private readonly BBoxSimilarityValueSource _enclosingInstance;
		    private readonly IRectangle rect;
		    private readonly double[] minX;
			private readonly double[] minY;
			private readonly double[] maxX;
			private readonly double[] maxY;

			private readonly IBits validMinX, validMaxX;

			public BBoxSimilarityValueSourceDocValues(IndexReader reader, BBoxSimilarityValueSource enclosingInstance, IState state)
			{
				_enclosingInstance = enclosingInstance;
                rect = _enclosingInstance.strategy.GetSpatialContext().MakeRectangle(0, 0, 0, 0); //reused

			    minX = FieldCache_Fields.DEFAULT.GetDoubles(reader, enclosingInstance.strategy.field_minX/*, true*/, state);
				minY = FieldCache_Fields.DEFAULT.GetDoubles(reader, enclosingInstance.strategy.field_minY/*, true*/, state);
				maxX = FieldCache_Fields.DEFAULT.GetDoubles(reader, enclosingInstance.strategy.field_maxX/*, true*/, state);
				maxY = FieldCache_Fields.DEFAULT.GetDoubles(reader, enclosingInstance.strategy.field_maxY/*, true*/, state);

				validMinX = FieldCache_Fields.DEFAULT.GetDocsWithField(reader, enclosingInstance.strategy.field_minX, state);
				validMaxX = FieldCache_Fields.DEFAULT.GetDocsWithField(reader, enclosingInstance.strategy.field_maxX, state);
			}

            public override float FloatVal(int doc)
            {
                // make sure it has minX and area
                if (validMinX.Get(doc) && validMaxX.Get(doc))
                {
                    rect.Reset(
                        minX[doc], maxX[doc],
                        minY[doc], maxY[doc]);
                    return (float) _enclosingInstance.similarity.Score(rect, null);
                }
                else
                {
                    return (float) _enclosingInstance.similarity.Score(null, null);
                }
            }

		    public override Explanation Explain(int doc)
			{
				// make sure it has minX and area
				if (validMinX.Get(doc) && validMaxX.Get(doc))
				{
					rect.Reset(
						minX[doc], maxX[doc],
						minY[doc], maxY[doc]);
					var exp = new Explanation();
					_enclosingInstance.similarity.Score(rect, exp);
					return exp;
				}
				return new Explanation(0, "No BBox");
			}

			public override string ToString(int doc)
			{
				return _enclosingInstance.Description() + "=" + FloatVal(doc);
			}
		}

		public override DocValues GetValues(IndexReader reader, IState state)
		{
			return new BBoxSimilarityValueSourceDocValues(reader, this, state);
		}

		public override string Description()
		{
			return "BBoxSimilarityValueSource(" + similarity + ")";
		}

		public override bool Equals(object o)
		{
			var other = o as BBoxSimilarityValueSource;
			if (other == null) return false;
			return similarity.Equals(other.similarity);
		}

		public override int GetHashCode()
		{
			return typeof(BBoxSimilarityValueSource).GetHashCode() + similarity.GetHashCode();
		}
	}
}
