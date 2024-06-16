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
using Lucene.Net.Index;
using Lucene.Net.Store;
using IndexReader = Lucene.Net.Index.IndexReader;
using TermDocs = Lucene.Net.Index.TermDocs;
using ToStringUtils = Lucene.Net.Util.ToStringUtils;

namespace Lucene.Net.Search
{

    /// <summary> A query that matches all documents.
    /// 
    /// </summary>

        [Serializable]
    public class MatchAllDocsQuery:Query
	{
		
		public MatchAllDocsQuery():this(null)
		{
		}
		
		private System.String normsField;
		
		/// <param name="normsField">Field used for normalization factor (document boost). Null if nothing.
		/// </param>
		public MatchAllDocsQuery(System.String normsField)
		{
			this.normsField = normsField;
		}
		
		private class MatchAllScorer:Scorer
		{
			private void  InitBlock(MatchAllDocsQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private MatchAllDocsQuery enclosingInstance;
			public MatchAllDocsQuery Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal TermDocs termDocs;
			internal float score;
			internal byte[] norms;
			private int doc = - 1;
			
			internal MatchAllScorer(MatchAllDocsQuery enclosingInstance, IndexReader reader, Similarity similarity, Weight w, byte[] norms, IState state) :base(similarity)
			{
				InitBlock(enclosingInstance);
				this.termDocs = reader.TermDocs(null, state);
				score = w.Value;
				this.norms = norms;
			}
			
			public override int DocID()
			{
				return doc;
			}
			
			public override int NextDoc(IState state)
			{
				return doc = termDocs.Next(state)?termDocs.Doc:NO_MORE_DOCS;
			}
			
			public override float Score(IState state)
			{
				return norms == null?score:score * Similarity.DecodeNorm(norms[DocID()]);
			}
			
			public override int Advance(int target, IState state)
			{
				return doc = termDocs.SkipTo(target, state)?termDocs.Doc:NO_MORE_DOCS;
			}
		}


        [Serializable]
        private class MatchAllDocsWeight:Weight
		{
			private void  InitBlock(MatchAllDocsQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private MatchAllDocsQuery enclosingInstance;
			public MatchAllDocsQuery Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private Similarity similarity;
			private float queryWeight;
			private float queryNorm;
			
			public MatchAllDocsWeight(MatchAllDocsQuery enclosingInstance, Searcher searcher)
			{
				InitBlock(enclosingInstance);
				this.similarity = searcher.Similarity;
			}
			
			public override System.String ToString()
			{
				return "weight(" + Enclosing_Instance + ")";
			}

		    public override Query Query
		    {
		        get { return Enclosing_Instance; }
		    }

		    public override float Value
		    {
		        get { return queryWeight; }
		    }

		    public override float GetSumOfSquaredWeights()
		    {
		        queryWeight = Enclosing_Instance.Boost;
		        return queryWeight*queryWeight;
		    }

		    public override void  Normalize(float queryNorm)
			{
				this.queryNorm = queryNorm;
				queryWeight *= this.queryNorm;
			}
			
			public override Scorer Scorer(IndexReader reader, bool scoreDocsInOrder, bool topScorer, IState state)
			{
				return new MatchAllScorer(enclosingInstance, reader, similarity, this, Enclosing_Instance.normsField != null?reader.Norms(Enclosing_Instance.normsField, state):null, state);
			}
			
			public override Explanation Explain(IndexReader reader, int doc, IState state)
			{
				// explain query weight
				Explanation queryExpl = new ComplexExplanation(true, Value, "MatchAllDocsQuery, product of:");
				if (Enclosing_Instance.Boost != 1.0f)
				{
					queryExpl.AddDetail(new Explanation(Enclosing_Instance.Boost, "boost"));
				}
				queryExpl.AddDetail(new Explanation(queryNorm, "queryNorm"));
				
				return queryExpl;
			}
		}
		
		public override Weight CreateWeight(Searcher searcher, IState state)
		{
			return new MatchAllDocsWeight(this, searcher);
		}
		
		public override void  ExtractTerms(System.Collections.Generic.ISet<Term> terms)
		{
		}
		
		public override System.String ToString(System.String field)
		{
			System.Text.StringBuilder buffer = new System.Text.StringBuilder();
			buffer.Append("*:*");
			buffer.Append(ToStringUtils.Boost(Boost));
			return buffer.ToString();
		}
		
		public  override bool Equals(System.Object o)
		{
			if (!(o is MatchAllDocsQuery))
				return false;
			MatchAllDocsQuery other = (MatchAllDocsQuery) o;
			return this.Boost == other.Boost;
		}
		
		public override int GetHashCode()
		{
			return BitConverter.ToInt32(BitConverter.GetBytes(Boost), 0) ^ 0x1AA71190;
		}
	}
}