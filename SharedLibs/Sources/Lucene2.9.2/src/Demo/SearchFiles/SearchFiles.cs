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

using Analyzer = Lucene.Net.Analysis.Analyzer;
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using FilterIndexReader = Lucene.Net.Index.FilterIndexReader;
using IndexReader = Lucene.Net.Index.IndexReader;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using FSDirectory = Lucene.Net.Store.FSDirectory;
using Version = Lucene.Net.Util.Version;
using Collector = Lucene.Net.Search.Collector;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Query = Lucene.Net.Search.Query;
using ScoreDoc = Lucene.Net.Search.ScoreDoc;
using Scorer = Lucene.Net.Search.Scorer;
using Searcher = Lucene.Net.Search.Searcher;
using TopScoreDocCollector = Lucene.Net.Search.TopScoreDocCollector;

namespace Lucene.Net.Demo
{
	
	/// <summary>Simple command-line based search demo. </summary>
	public class SearchFiles
	{
		private class AnonymousClassCollector:Collector
		{
			private Scorer scorer;
			private int docBase;
			
			// simply print docId and score of every matching document
			public override void  Collect(int doc)
			{
				System.Console.Out.WriteLine("doc=" + doc + docBase + " score=" + scorer.Score());
			}
			
			public override bool AcceptsDocsOutOfOrder()
			{
				return true;
			}
			
			public override void  SetNextReader(IndexReader reader, int docBase)
			{
				this.docBase = docBase;
			}
			
			public override void  SetScorer(Scorer scorer)
			{
				this.scorer = scorer;
			}
		}
		
		/// <summary>Use the norms from one field for all fields.  Norms are read into memory,
		/// using a byte of memory per document per searched field.  This can cause
		/// search of large collections with a large number of fields to run out of
		/// memory.  If all of the fields contain only a single token, then the norms
		/// are all identical, then single norm vector may be shared. 
		/// </summary>
		private class OneNormsReader:FilterIndexReader
		{
			private System.String field;
			
			public OneNormsReader(IndexReader in_Renamed, System.String field):base(in_Renamed)
			{
				this.field = field;
			}
			
			public override byte[] Norms(System.String field)
			{
				return in_Renamed.Norms(this.field);
			}
		}
		
		private SearchFiles()
		{
		}
		
		/// <summary>Simple command-line based search demo. </summary>
		[STAThread]
		public static void  Main(System.String[] args)
		{
			System.String usage = "Usage:\t" + typeof(SearchFiles) + "[-index dir] [-field f] [-repeat n] [-queries file] [-raw] [-norms field] [-paging hitsPerPage]";
			usage += "\n\tSpecify 'false' for hitsPerPage to use streaming instead of paging search.";
			if (args.Length > 0 && ("-h".Equals(args[0]) || "-help".Equals(args[0])))
			{
				System.Console.Out.WriteLine(usage);
				System.Environment.Exit(0);
			}
			
			System.String index = "index";
			System.String field = "contents";
			System.String queries = null;
			int repeat = 0;
			bool raw = false;
			System.String normsField = null;
			bool paging = true;
			int hitsPerPage = 10;
			
			for (int i = 0; i < args.Length; i++)
			{
				if ("-index".Equals(args[i]))
				{
					index = args[i + 1];
					i++;
				}
				else if ("-field".Equals(args[i]))
				{
					field = args[i + 1];
					i++;
				}
				else if ("-queries".Equals(args[i]))
				{
					queries = args[i + 1];
					i++;
				}
				else if ("-repeat".Equals(args[i]))
				{
					repeat = System.Int32.Parse(args[i + 1]);
					i++;
				}
				else if ("-raw".Equals(args[i]))
				{
					raw = true;
				}
				else if ("-norms".Equals(args[i]))
				{
					normsField = args[i + 1];
					i++;
				}
				else if ("-paging".Equals(args[i]))
				{
					if (args[i + 1].Equals("false"))
					{
						paging = false;
					}
					else
					{
						hitsPerPage = System.Int32.Parse(args[i + 1]);
						if (hitsPerPage == 0)
						{
							paging = false;
						}
					}
					i++;
				}
			}
			
			IndexReader reader = IndexReader.Open(FSDirectory.Open(new System.IO.FileInfo(index)), true); // only searching, so read-only=true
			
			if (normsField != null)
				reader = new OneNormsReader(reader, normsField);
			
			Searcher searcher = new IndexSearcher(reader);
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
			
			System.IO.StreamReader in_Renamed = null;
			if (queries != null)
			{
				in_Renamed = new System.IO.StreamReader(new System.IO.StreamReader(queries, System.Text.Encoding.Default).BaseStream, new System.IO.StreamReader(queries, System.Text.Encoding.Default).CurrentEncoding);
			}
			else
			{
				in_Renamed = new System.IO.StreamReader(new System.IO.StreamReader(System.Console.OpenStandardInput(), System.Text.Encoding.GetEncoding("UTF-8")).BaseStream, new System.IO.StreamReader(System.Console.OpenStandardInput(), System.Text.Encoding.GetEncoding("UTF-8")).CurrentEncoding);
			}
			QueryParser parser = new QueryParser(field, analyzer);
			while (true)
			{
				if (queries == null)
				// prompt the user
					System.Console.Out.WriteLine("Enter query: ");
				
				System.String line = in_Renamed.ReadLine();
				
				if (line == null || line.Length == - 1)
					break;
				
				line = line.Trim();
				if (line.Length == 0)
					break;
				
				Query query = parser.Parse(line);
				System.Console.Out.WriteLine("Searching for: " + query.ToString(field));
				
				
				if (repeat > 0)
				{
					// repeat & time as benchmark
					System.DateTime start = System.DateTime.Now;
					for (int i = 0; i < repeat; i++)
					{
						searcher.Search(query, null, 100);
					}
					System.DateTime end = System.DateTime.Now;
					System.Console.Out.WriteLine("Time: " + (end.Millisecond - start.Millisecond) + "ms");
				}
				
				if (paging)
				{
					DoPagingSearch(in_Renamed, searcher, query, hitsPerPage, raw, queries == null);
				}
				else
				{
					DoStreamingSearch(searcher, query);
				}
			}
			reader.Close();
		}
		
		/// <summary> This method uses a custom HitCollector implementation which simply prints out
		/// the docId and score of every matching document. 
		/// 
		/// This simulates the streaming search use case, where all hits are supposed to
		/// be processed, regardless of their relevance.
		/// </summary>
		public static void  DoStreamingSearch(Searcher searcher, Query query)
		{
			Collector streamingHitCollector = new AnonymousClassCollector();
			
			searcher.Search(query, streamingHitCollector);
		}
		
		/// <summary> This demonstrates a typical paging search scenario, where the search engine presents 
		/// pages of size n to the user. The user can then go to the next page if interested in
		/// the next hits.
		/// 
		/// When the query is executed for the first time, then only enough results are collected
		/// to fill 5 result pages. If the user wants to page beyond this limit, then the query
		/// is executed another time and all hits are collected.
		/// 
		/// </summary>
		public static void  DoPagingSearch(System.IO.StreamReader in_Renamed, Searcher searcher, Query query, int hitsPerPage, bool raw, bool interactive)
		{
			
			// Collect enough docs to show 5 pages
			TopScoreDocCollector collector = TopScoreDocCollector.create(5 * hitsPerPage, false);
			searcher.Search(query, collector);
			ScoreDoc[] hits = collector.TopDocs().scoreDocs;
			
			int numTotalHits = collector.GetTotalHits();
			System.Console.Out.WriteLine(numTotalHits + " total matching documents");
			
			int start = 0;
			int end = System.Math.Min(numTotalHits, hitsPerPage);
			
			while (true)
			{
				if (end > hits.Length)
				{
					System.Console.Out.WriteLine("Only results 1 - " + hits.Length + " of " + numTotalHits + " total matching documents collected.");
					System.Console.Out.WriteLine("Collect more (y/n) ?");
					System.String line = in_Renamed.ReadLine();
					if (line.Length == 0 || line[0] == 'n')
					{
						break;
					}
					
					collector = TopScoreDocCollector.create(numTotalHits, false);
					searcher.Search(query, collector);
					hits = collector.TopDocs().scoreDocs;
				}
				
				end = System.Math.Min(hits.Length, start + hitsPerPage);
				
				for (int i = start; i < end; i++)
				{
					if (raw)
					{
						// output raw format
						System.Console.Out.WriteLine("doc=" + hits[i].doc + " score=" + hits[i].score);
						continue;
					}
					
					Document doc = searcher.Doc(hits[i].doc);
					System.String path = doc.Get("path");
					if (path != null)
					{
						System.Console.Out.WriteLine((i + 1) + ". " + path);
						System.String title = doc.Get("title");
						if (title != null)
						{
							System.Console.Out.WriteLine("   Title: " + doc.Get("title"));
						}
					}
					else
					{
						System.Console.Out.WriteLine((i + 1) + ". " + "No path for this document");
					}
				}
				
				if (!interactive)
				{
					break;
				}
				
				if (numTotalHits >= end)
				{
					bool quit = false;
					while (true)
					{
						System.Console.Out.Write("Press ");
						if (start - hitsPerPage >= 0)
						{
							System.Console.Out.Write("(p)revious page, ");
						}
						if (start + hitsPerPage < numTotalHits)
						{
							System.Console.Out.Write("(n)ext page, ");
						}
						System.Console.Out.WriteLine("(q)uit or enter number to jump to a page.");
						
						System.String line = in_Renamed.ReadLine();
						if (line.Length == 0 || line[0] == 'q')
						{
							quit = true;
							break;
						}
						if (line[0] == 'p')
						{
							start = System.Math.Max(0, start - hitsPerPage);
							break;
						}
						else if (line[0] == 'n')
						{
							if (start + hitsPerPage < numTotalHits)
							{
								start += hitsPerPage;
							}
							break;
						}
						else
						{
							int page = System.Int32.Parse(line);
							if ((page - 1) * hitsPerPage < numTotalHits)
							{
								start = (page - 1) * hitsPerPage;
								break;
							}
							else
							{
								System.Console.Out.WriteLine("No such page");
							}
						}
					}
					if (quit)
						break;
					end = System.Math.Min(numTotalHits, start + hitsPerPage);
				}
			}
		}
	}
}