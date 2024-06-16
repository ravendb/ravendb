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
using Lucene.Net.Search.Function;
using Lucene.Net.Store;

namespace Lucene.Net.Spatial.Util
{
	/// <summary>
	/// Port of Solr's FunctionQuery (v1.4)
	/// 
	/// Returns a score for each document based on a ValueSource,
	/// often some function of the value of a field.
	/// 
	/// <b>Note: This API is experimental and may change in non backward-compatible ways in the future</b>
	/// </summary>
	public class FunctionQuery : Query
	{
		protected readonly ValueSource func;

		public FunctionQuery(ValueSource func)
		{
			this.func = func;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns>The associated ValueSource</returns>
		public ValueSource GetValueSource()
		{
			return func;
		}

		public override Query Rewrite(Index.IndexReader reader, IState state)
		{
			return this;
		}

		public override void ExtractTerms(System.Collections.Generic.ISet<Term> terms)
		{
			//base.ExtractTerms(terms);
		}

		protected class FunctionWeight : Weight
		{
			protected Searcher searcher;
			protected float queryNorm;
			protected float queryWeight;
			protected readonly FunctionQuery enclosingInstance;

			public FunctionWeight(Searcher searcher, FunctionQuery q)
			{
				enclosingInstance = q;
				this.searcher = searcher;
				//q.func.CreateWeight(searcher);
			}

			internal float GetQueryNorm()
			{
				return queryNorm;
			}

			public override Query Query
			{
				get { return enclosingInstance; }
			}

			public override float Value
			{
				get { return queryWeight; }
			}

			public override float GetSumOfSquaredWeights()
			{
				queryWeight = enclosingInstance.Boost;
				return queryWeight * queryWeight;
			}

			public override void Normalize(float norm)
			{
				this.queryNorm = norm;
				queryWeight *= this.queryNorm;
			}

			public override Scorer Scorer(IndexReader reader, bool scoreDocsInOrder, bool topScorer, IState state)
			{
				return new AllScorer(enclosingInstance.GetSimilarity(searcher), reader, this, state);
			}

			public override Explanation Explain(IndexReader reader, int doc, IState state)
			{
				return ((AllScorer)Scorer(reader, true, true, state)).Explain(doc);
			}
		}

		protected class AllScorer : Scorer
		{
			readonly IndexReader reader;
			readonly FunctionWeight weight;
			readonly int maxDoc;
			readonly float qWeight;
			int doc = -1;
			readonly DocValues vals;
			readonly bool hasDeletions;

			public AllScorer(Similarity similarity, IndexReader reader, FunctionWeight w, IState state)
				: base(similarity)
			{
				this.weight = w;
				this.qWeight = w.Value;
				this.reader = reader;
				this.maxDoc = reader.MaxDoc;
				this.hasDeletions = reader.HasDeletions;
				vals = ((FunctionQuery)w.Query).func.GetValues(reader, state);
			}

			public override int DocID()
			{
				return doc;
			}

			// instead of matching all docs, we could also embed a query.
			// the score could either ignore the subscore, or boost it.
			// Containment:  floatline(foo:myTerm, "myFloatField", 1.0, 0.0f)
			// Boost:        foo:myTerm^floatline("myFloatField",1.0,0.0f)
			public override int NextDoc(IState state)
			{
				for (; ; )
				{
					++doc;
					if (doc >= maxDoc)
					{
						return doc = NO_MORE_DOCS;
					}
					if (hasDeletions && reader.IsDeleted(doc)) continue;
					return doc;
				}
			}

			public override int Advance(int target, IState state)
			{
				// this will work even if target==NO_MORE_DOCS
				doc = target - 1;
				return NextDoc(state);
			}

			public override float Score(IState state)
			{
				float score = qWeight * vals.FloatVal(doc);

				// Current Lucene priority queues can't handle NaN and -Infinity, so
				// map to -Float.MAX_VALUE. This conditional handles both -infinity
				// and NaN since comparisons with NaN are always false.
				return score > float.NegativeInfinity ? score : -float.MaxValue;
			}

			public /*override*/ Explanation Explain(int doc)
			{
				float sc = qWeight * vals.FloatVal(doc);

				Explanation result = new ComplexExplanation
				  (true, sc, "FunctionQuery(" + ((FunctionQuery)weight.Query).func + "), product of:");

				result.AddDetail(vals.Explain(doc));
				result.AddDetail(new Explanation(weight.Query.Boost, "boost"));
				result.AddDetail(new Explanation(weight.GetQueryNorm(), "queryNorm"));
				return result;
			}
		}

		public override Weight CreateWeight(Searcher searcher, IState state)
		{
			return new FunctionQuery.FunctionWeight(searcher, this);
		}

		public override string ToString(string field)
		{
			float boost = Boost;
			return (boost != 1.0 ? "(" : "") + func.ToString()
					+ (boost == 1.0 ? "" : ")^" + boost);
		}

		public override bool Equals(object o)
		{
			var other = o as FunctionQuery;

			if (other == null) return false;

			return this.Boost == other.Boost && this.func.Equals(other.func);
		}

		public override int GetHashCode()
		{
			return (int) (func.GetHashCode() * 31 + BitConverter.DoubleToInt64Bits(Boost));
		}
	}
}
