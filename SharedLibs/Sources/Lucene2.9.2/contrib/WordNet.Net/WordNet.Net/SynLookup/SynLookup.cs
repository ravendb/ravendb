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

namespace WorldNet.Net
{
	
	
	/// <summary> Test program to look up synonyms.</summary>
	public class SynLookup
	{
		
		[STAThread]
		public static void  Main(System.String[] args)
		{
			if (args.Length != 2)
			{
				System.Console.Out.WriteLine(typeof(SynLookup) + " <index path> <word>");
                return;
			}
			
			FSDirectory directory = FSDirectory.GetDirectory(args[0], false);
			IndexSearcher searcher = new IndexSearcher(directory);
			
			System.String word = args[1];
			Hits hits = searcher.Search(new TermQuery(new Term(Syns2Index.F_WORD, word)));
			
			if (hits.Length() == 0)
			{
				System.Console.Out.WriteLine("No synonyms found for " + word);
			}
			else
			{
				System.Console.Out.WriteLine("Synonyms found for \"" + word + "\":");
			}
			
			for (int i = 0; i < hits.Length(); i++)
			{
				Document doc = hits.Doc(i);
				
				System.String[] values = doc.GetValues(Syns2Index.F_SYN);
				
				for (int j = 0; j < values.Length; j++)
				{
					System.Console.Out.WriteLine(values[j]);
				}
			}
			
			searcher.Close();
			directory.Close();
		}
		
		
		/// <summary> Perform synonym expansion on a query.
		/// 
		/// </summary>
		/// <param name="">query
		/// </param>
		/// <param name="">syns
		/// </param>
		/// <param name="">a
		/// </param>
		/// <param name="">field
		/// </param>
		/// <param name="">boost
		/// </param>
		public static Query Expand(System.String query, Searcher syns, Analyzer a, System.String field, float boost)
		{
			System.Collections.Hashtable already = new System.Collections.Hashtable(); // avoid dups		
			System.Collections.IList top = new System.Collections.ArrayList(); // needs to be separately listed..
			
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