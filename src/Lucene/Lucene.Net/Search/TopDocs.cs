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

namespace Lucene.Net.Search
{

    /// <summary> Represents hits returned by <see cref="Searcher.Search(Query,Filter,int)" />
    /// and <see cref="Searcher.Search(Query,int)" />
    /// </summary>

        [Serializable]
    public class TopDocs
	{
		private int _totalHits;
        private ScoreDoc[] _scoreDocs;
        private float _maxScore;

        /// <summary>The total number of hits for the query.</summary>
        public int TotalHits
        {
            get { return _totalHits; }
            set { _totalHits = value; }
        }

        /// <summary>The top hits for the query. </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public ScoreDoc[] ScoreDocs
        {
            get { return _scoreDocs; }
            set { _scoreDocs = value; }
        }

        /// <summary>
        /// Gets or sets the maximum score value encountered, needed for normalizing.
        /// Note that in case scores are not tracked, this returns <see cref="float.NaN" />.
        /// </summary>
        public float MaxScore
        {
            get { return _maxScore; }
            set { _maxScore = value; }
        }
		
		/// <summary>Constructs a TopDocs with a default maxScore=Float.NaN. </summary>
		internal TopDocs(int totalHits, ScoreDoc[] scoreDocs):this(totalHits, scoreDocs, float.NaN)
		{
		}
		
		/// <summary></summary>
		public TopDocs(int totalHits, ScoreDoc[] scoreDocs, float maxScore)
		{
			this.TotalHits = totalHits;
			this.ScoreDocs = scoreDocs;
			this.MaxScore = maxScore;
		}
	}
}