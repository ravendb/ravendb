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

using Lucene.Net.Store;
using Lucene.Net.Search;
using Lucene.Net.Index;
using Lucene.Net.Documents;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;

namespace WorldNet.Net
{
	
	
	/// <summary> Expand a query by looking up synonyms for every term.
	/// You need to invoke {@link Syns2Index} first to build the synonym index.
	/// 
	/// </summary>
	/// <seealso cref="Syns2Index">
	/// </seealso>
	public sealed class SynExpand
	{
		
		/// <summary> Test driver for synonym expansion.
		/// Uses boost factor of 0.9 for illustrative purposes.
		/// 
		/// If you pass in the query "big dog" then it prints out:
		/// 
		/// <code><pre>
		/// Query: big adult^0.9 bad^0.9 bighearted^0.9 boastful^0.9 boastfully^0.9 bounteous^0.9 bountiful^0.9 braggy^0.9 crowing^0.9 freehanded^0.9 giving^0.9 grown^0.9 grownup^0.9 handsome^0.9 large^0.9 liberal^0.9 magnanimous^0.9 momentous^0.9 openhanded^0.9 prominent^0.9 swelled^0.9 vainglorious^0.9 vauntingly^0.9
		/// dog andiron^0.9 blackguard^0.9 bounder^0.9 cad^0.9 chase^0.9 click^0.9 detent^0.9 dogtooth^0.9 firedog^0.9 frank^0.9 frankfurter^0.9 frump^0.9 heel^0.9 hotdog^0.9 hound^0.9 pawl^0.9 tag^0.9 tail^0.9 track^0.9 trail^0.9 weenie^0.9 wiener^0.9 wienerwurst^0.9
		/// </pre></code>
		/// </summary>
		[STAThread]
		public static void  Main(System.String[] args)
		{
			if (args.Length != 2)
			{
				System.Console.Out.WriteLine(typeof(SynExpand) + " <index path> <query>");
                return;
			}
			
			FSDirectory directory = FSDirectory.GetDirectory(args[0], false);
			IndexSearcher searcher = new IndexSearcher(directory);
			
			System.String query = args[1];
			System.String field = "contents";
			
			Query q = Expand(query, searcher, new StandardAnalyzer(), field, 0.9f);
			System.Console.Out.WriteLine("Query: " + q.ToString(field));
			
			
			
			searcher.Close();
			directory.Close();
		}
		
		
		/// <summary> Perform synonym expansion on a query.
		/// 
		/// </summary>
		/// <param name="query">users query that is assumed to not have any "special" query syntax, thus it should be just normal words, so "big dog" makes sense, but a query like "title:foo^1.2" doesn't as this should presumably be passed directly to the default query parser.
		/// 
		/// </param>
		/// <param name="syns">a opened to the Lucene index you previously created with {@link Syns2Index}. The searcher is not closed or otherwise altered.
		/// 
		/// </param>
		/// <param name="a">optional analyzer used to parse the users query else {@link StandardAnalyzer} is used
		/// 
		/// </param>
		/// <param name="field">optional field name to search in or null if you want the default of "contents"
		/// 
		/// </param>
		/// <param name="boost">optional boost applied to synonyms else no boost is applied
		/// 
		/// </param>
		/// <returns> the expanded Query
		/// </returns>
		public static Query Expand(System.String query, Searcher syns, Analyzer a, System.String field, float boost)
		{
			System.Collections.Hashtable already = new System.Collections.Hashtable(); // avoid dups 
			System.Collections.IList top = new System.Collections.ArrayList(); // needs to be separately listed..
			if (field == null)
				field = "contents";
			if (a == null)
				a = new StandardAnalyzer();
			
			// [1] Parse query into separate words so that when we expand we can avoid dups
			TokenStream ts = a.TokenStream(field, new System.IO.StringReader(query));
			Lucene.Net.Analysis.Token t;
			while ((t = ts.Next()) != null)
			{
				System.String word = t.TermText();
				if (already.Contains(word) == false)
				{
					already.Add(word, word);
					top.Add(word);
				}
			}
			BooleanQuery tmp = new BooleanQuery();
			
			// [2] form query
			System.Collections.IEnumerator it = top.GetEnumerator();
			while (it.MoveNext())
			{
				// [2a] add to level words in
				System.String word = (System.String) it.Current;
				TermQuery tq = new TermQuery(new Term(field, word));
				tmp.Add(tq, BooleanClause.Occur.SHOULD);
				
				// [2b] add in unique synonums
				Hits hits = syns.Search(new TermQuery(new Term(Syns2Index.F_WORD, word)));
				for (int i = 0; i < hits.Length(); i++)
				{
					Document doc = hits.Doc(i);
					System.String[] values = doc.GetValues(Syns2Index.F_SYN);
					for (int j = 0; j < values.Length; j++)
					{
						System.String syn = values[j];
						if (already.Contains(syn) == false)
						// avoid dups of top level words and synonyms
						{
							already.Add(syn, syn);
							tq = new TermQuery(new Term(field, syn));
							if (boost > 0)
							// else keep normal 1.0
								tq.SetBoost(boost);
							tmp.Add(tq, BooleanClause.Occur.SHOULD);
						}
					}
				}
			}
			
			
			return tmp;
		}
	}


	/// <summary>
	/// From project WordNet.Net.Syns2Index
	/// </summary>
	public class Syns2Index
	{
		/// <summary> </summary>
		public const System.String F_SYN = "syn";

		/// <summary> </summary>
		public const System.String F_WORD = "word";
	}
}