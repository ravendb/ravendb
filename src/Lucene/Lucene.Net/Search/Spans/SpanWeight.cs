/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using Lucene.Net.Index;
using IndexReader = Lucene.Net.Index.IndexReader;
using Lucene.Net.Search;
using Lucene.Net.Store;
using IDFExplanation = Lucene.Net.Search.Explanation.IDFExplanation;

namespace Lucene.Net.Search.Spans
{

    /// <summary> Expert-only.  Public for use by other weight implementations</summary>
    [Serializable]
    public class SpanWeight:Weight
	{
		protected internal Similarity similarity;
		protected internal float value_Renamed;
		protected internal float idf;
		protected internal float queryNorm;
		protected internal float queryWeight;

        protected internal ISet<Term> terms;
		protected internal SpanQuery internalQuery;
		private IDFExplanation idfExp;
		
		public SpanWeight(SpanQuery query, Searcher searcher, IState state)
		{
			this.similarity = query.GetSimilarity(searcher);
			this.internalQuery = query;

		    terms = Lucene.Net.Support.Compatibility.SetFactory.CreateHashSet<Term>();
			query.ExtractTerms(terms);

			idfExp = similarity.IdfExplain(terms, searcher, state);
			idf = idfExp.Idf;
		}

	    public override Query Query
	    {
	        get { return internalQuery; }
	    }

	    public override float Value
	    {
	        get { return value_Renamed; }
	    }

	    public override float GetSumOfSquaredWeights()
	    {
	        queryWeight = idf*internalQuery.Boost; // compute query weight
	        return queryWeight*queryWeight; // square it
	    }

	    public override void  Normalize(float queryNorm)
		{
			this.queryNorm = queryNorm;
			queryWeight *= queryNorm; // normalize query weight
			value_Renamed = queryWeight * idf; // idf for document
		}
		
		public override Scorer Scorer(IndexReader reader, bool scoreDocsInOrder, bool topScorer, IState state)
		{
			return new SpanScorer(internalQuery.GetSpans(reader, state), this, similarity, reader.Norms(internalQuery.Field, state), state);
		}
		
		public override Explanation Explain(IndexReader reader, int doc, IState state)
		{
			
			ComplexExplanation result = new ComplexExplanation();
			result.Description = "weight(" + Query + " in " + doc + "), product of:";
			System.String field = ((SpanQuery) Query).Field;
			
			Explanation idfExpl = new Explanation(idf, "idf(" + field + ": " + idfExp.Explain() + ")");
			
			// explain query weight
			Explanation queryExpl = new Explanation();
			queryExpl.Description = "queryWeight(" + Query + "), product of:";
			
			Explanation boostExpl = new Explanation(Query.Boost, "boost");
			if (Query.Boost != 1.0f)
				queryExpl.AddDetail(boostExpl);
			queryExpl.AddDetail(idfExpl);
			
			Explanation queryNormExpl = new Explanation(queryNorm, "queryNorm");
			queryExpl.AddDetail(queryNormExpl);
			
			queryExpl.Value = boostExpl.Value * idfExpl.Value * queryNormExpl.Value;
			
			result.AddDetail(queryExpl);
			
			// explain field weight
			ComplexExplanation fieldExpl = new ComplexExplanation();
			fieldExpl.Description = "fieldWeight(" + field + ":" + internalQuery.ToString(field) + " in " + doc + "), product of:";
			
			Explanation tfExpl = ((SpanScorer)Scorer(reader, true, false, state)).Explain(doc, state);
			fieldExpl.AddDetail(tfExpl);
			fieldExpl.AddDetail(idfExpl);
			
			Explanation fieldNormExpl = new Explanation();
			byte[] fieldNorms = reader.Norms(field, state);
			float fieldNorm = fieldNorms != null?Similarity.DecodeNorm(fieldNorms[doc]):1.0f;
			fieldNormExpl.Value = fieldNorm;
			fieldNormExpl.Description = "fieldNorm(field=" + field + ", doc=" + doc + ")";
			fieldExpl.AddDetail(fieldNormExpl);
			
			fieldExpl.Match = tfExpl.IsMatch;
			fieldExpl.Value = tfExpl.Value * idfExpl.Value * fieldNormExpl.Value;
			
			result.AddDetail(fieldExpl);
			System.Boolean? tempAux = fieldExpl.Match;
			result.Match = tempAux;
			
			// combine them
			result.Value = queryExpl.Value * fieldExpl.Value;
			
			if (queryExpl.Value == 1.0f)
				return fieldExpl;
			
			return result;
		}
	}
}