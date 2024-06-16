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
using System.IO;
using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace WorldNet.Net
{
	/// <summary> Test program to look up synonyms.</summary>
	public class SynLookup
	{
		static List<String> already;
		private static BooleanQuery tmp;

		[STAThread]
		public static void  Main(System.String[] args)
		{
			if (args.Length != 2)
			{
				System.Console.Out.WriteLine(typeof(SynLookup) + " <index path> <word>");
				return;
			}
			
			using (var directory = FSDirectory.Open(new DirectoryInfo(args[0])))
			{
				using (var searcher = new IndexSearcher(directory, true))
				{

					String word = args[1];
					Query query = new TermQuery(new Term(Syns2Index.F_WORD, word));
					var countingCollector = new CountingCollector();
					searcher.Search(query, countingCollector);

					if (countingCollector.numHits == 0)
					{
						Console.Out.WriteLine("No synonyms found for " + word);
					}
					else
					{
						Console.Out.WriteLine("Synonyms found for \"" + word + "\":");
					}

					var hits = searcher.Search(query, countingCollector.numHits).ScoreDocs;

					foreach (var v in
						hits.Select(t => searcher.Doc(t.Doc)).Select(doc => doc.GetValues(Syns2Index.F_SYN)).SelectMany(values => values))
					{
						Console.Out.WriteLine(v);
					}

				}
			}
		}
		
		/// <summary> 
		/// Perform synonym expansion on a query.
		/// </summary>
		/// <param name="query">query</param>
		/// <param name="syns">syns</param>
		/// <param name="a">a</param>
		/// <param name="field">field</param>
		/// <param name="boost">boost</param>
		public static Query Expand(String query, 
			Searcher syns, 
			Analyzer a, 
			String field, 
			float boost)
		{
			already = new List<String>(); // avoid dups		
			var top = new List<String>(); // needs to be separately listed..

			var ts = a.TokenStream(field, new StringReader(query));
			var termAtt = ts.AddAttribute<TermAttribute>();

			while (ts.IncrementToken())
			{
				var word = termAtt.Term;

				if (!already.Contains(word))
				{
					already.Add(word);
					top.Add(word);
				}
			}

			tmp = new BooleanQuery();

			// [2] form query
			System.Collections.IEnumerator it = top.GetEnumerator();
			while (it.MoveNext())
			{
				// [2a] add to level words in
				var word = (String)it.Current;
				var tq = new TermQuery(new Term(field, word));
				tmp.Add(tq, Occur.SHOULD);

				var c = new CollectorImpl(field, boost);
				syns.Search(new TermQuery(new Term(Syns2Index.F_WORD, word)), c);
			}

			return tmp;
		}

        internal sealed class CountingCollector : Collector
        {
            public int numHits;

            public override void SetScorer(Scorer scorer)
            { }

            public override void Collect(int doc)
            {
                numHits++;
            }

            public override void SetNextReader(IndexReader reader, int docBase)
            { }

            public override bool AcceptsDocsOutOfOrder
            {
                get { return true; }
            }
        }

        /// <summary>
        /// CollectorImpl
        /// </summary>
        internal sealed class CollectorImpl : Collector
        {
            private IndexReader reader;
            private readonly string field;
            private readonly float boost;

            public CollectorImpl(string field, float boost)
            {
                this.field = field;
                this.boost = boost;
            }

            public override void SetScorer(Scorer scorer)
            {
                // Ignore
            }

            public override void Collect(int doc)
            {
                var d = reader.Document(doc);
                var values = d.GetValues(Syns2Index.F_SYN);
                foreach (var syn in values.Where(syn => !already.Contains(syn)))
                {
                    already.Add(syn);

                    var tq = new TermQuery(new Term(field, syn));
                    if (boost > 0) // else keep normal 1.0
                        tq.Boost = boost;

                    tmp.Add(tq, Occur.SHOULD);
                }
            }

            public override void SetNextReader(IndexReader reader, int docBase)
            {
                this.reader = reader;
            }

            public override bool AcceptsDocsOutOfOrder
            {
                get { return true; }
            }

        }

        /// <summary>
        /// From project WordNet.Net.Syns2Index
        /// </summary>
        public class Syns2Index
        {
            /// <summary> </summary>
            public const String F_SYN = "syn";

            /// <summary> </summary>
            public const String F_WORD = "word";
        }

    }

}