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
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace WorldNet.Net
{


    /// <summary> Expand a query by looking up synonyms for every term.
    /// You need to invoke <see cref="Syns2Index"/> first to build the synonym index.
    /// 
    /// </summary>
    /// <seealso cref="Syns2Index" />
    public sealed class SynExpand
    {
        static List<String> already;
        private static BooleanQuery tmp;

        /// <summary> Test driver for synonym expansion.
        /// Uses boost factor of 0.9 for illustrative purposes.
        /// 
        /// If you pass in the query "big dog" then it prints out:
        /// 
        /// <pre>
        /// Query: big adult^0.9 bad^0.9 bighearted^0.9 boastful^0.9 boastfully^0.9 bounteous^0.9 bountiful^0.9 braggy^0.9 crowing^0.9 freehanded^0.9 giving^0.9 grown^0.9 grownup^0.9 handsome^0.9 large^0.9 liberal^0.9 magnanimous^0.9 momentous^0.9 openhanded^0.9 prominent^0.9 swelled^0.9 vainglorious^0.9 vauntingly^0.9
        /// dog andiron^0.9 blackguard^0.9 bounder^0.9 cad^0.9 chase^0.9 click^0.9 detent^0.9 dogtooth^0.9 firedog^0.9 frank^0.9 frankfurter^0.9 frump^0.9 heel^0.9 hotdog^0.9 hound^0.9 pawl^0.9 tag^0.9 tail^0.9 track^0.9 trail^0.9 weenie^0.9 wiener^0.9 wienerwurst^0.9
        /// </pre>
        /// </summary>
        [STAThread]
        public static void Main(String[] args)
        {
            if (args.Length != 2)
            {
                Console.Out.WriteLine(typeof(SynExpand) + " <index path> <query>");
                return;
            }

            var directory = FSDirectory.Open(new DirectoryInfo(args[0]));
            var searcher = new IndexSearcher(directory, true);

            String query = args[1];
            const string field = "contents";

            Query q = Expand(query, searcher, new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_CURRENT), field, 0.9f);
            System.Console.Out.WriteLine("Query: " + q.ToString(field));

            searcher.Close();
            directory.Close();
        }


        /// <summary> 
        /// Perform synonym expansion on a query.
        /// </summary>
        /// <param name="query">users query that is assumed to not have any "special" query syntax, thus it should be just normal words, so "big dog" makes sense, but a query like "title:foo^1.2" doesn't as this should presumably be passed directly to the default query parser </param>
        /// <param name="syns">a opened to the Lucene index you previously created with <see cref="Syns2Index"/>. The searcher is not closed or otherwise altered. </param>
        /// <param name="a">optional analyzer used to parse the users query else <see cref="StandardAnalyzer"/> is used </param>
        /// <param name="field">optional field name to search in or null if you want the default of "contents" </param>
        /// <param name="boost">optional boost applied to synonyms else no boost is applied </param>
        /// <returns>the expanded Query </returns>
        public static Query Expand(String query,
            Searcher syns,
            Analyzer a,
            String field,
            float boost)
		{
			already = new List<String>(); // avoid dups 
			var top = new List<String>(); // needs to be separately listed..
			if (field == null)
				field = "contents";
			
            if (a == null)
				a = new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_CURRENT);
			
			// [1] Parse query into separate words so that when we expand we can avoid dups
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
				var word = (String) it.Current;
				var tq = new TermQuery(new Term(field, word));
				tmp.Add(tq, Occur.SHOULD);

			    var c = new CollectorImpl(field, boost);
                syns.Search(new TermQuery(new Term(Syns2Index.F_WORD, word)), c);
			}
			
			return tmp;
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

    }
}