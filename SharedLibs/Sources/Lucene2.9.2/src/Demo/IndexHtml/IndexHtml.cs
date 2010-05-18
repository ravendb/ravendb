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

using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using TermEnum = Lucene.Net.Index.TermEnum;
using FSDirectory = Lucene.Net.Store.FSDirectory;
using Version = Lucene.Net.Util.Version;

namespace Lucene.Net.Demo
{
	
	/// <summary>Indexer for HTML files. </summary>
	public class IndexHTML
	{
		private IndexHTML()
		{
		}
		
		private static bool deleting = false; // true during deletion pass
		private static IndexReader reader; // existing index
		private static IndexWriter writer; // new index being built
		private static TermEnum uidIter; // document id iterator
		
		/// <summary>Indexer for HTML files.</summary>
		[STAThread]
		public static void  Main(System.String[] argv)
		{
			try
			{
				System.IO.FileInfo index = new System.IO.FileInfo("index");
				bool create = false;
				System.IO.FileInfo root = null;
				
				System.String usage = "IndexHTML [-create] [-index <index>] <root_directory>";
				
				if (argv.Length == 0)
				{
					System.Console.Error.WriteLine("Usage: " + usage);
					return ;
				}
				
				for (int i = 0; i < argv.Length; i++)
				{
					if (argv[i].Equals("-index"))
					{
						// parse -index option
						index = new System.IO.FileInfo(argv[++i]);
					}
					else if (argv[i].Equals("-create"))
					{
						// parse -create option
						create = true;
					}
					else if (i != argv.Length - 1)
					{
						System.Console.Error.WriteLine("Usage: " + usage);
						return ;
					}
					else
						root = new System.IO.FileInfo(argv[i]);
				}
				
				if (root == null)
				{
					System.Console.Error.WriteLine("Specify directory to index");
					System.Console.Error.WriteLine("Usage: " + usage);
					return ;
				}
				
				System.DateTime start = System.DateTime.Now;
				
				if (!create)
				{
					// delete stale docs
					deleting = true;
					IndexDocs(root, index, create);
				}
				writer = new IndexWriter(FSDirectory.Open(index), new StandardAnalyzer(Version.LUCENE_CURRENT), create, new IndexWriter.MaxFieldLength(1000000));
				IndexDocs(root, index, create); // add new docs
				
				System.Console.Out.WriteLine("Optimizing index...");
				writer.Optimize();
				writer.Close();
				
				System.DateTime end = System.DateTime.Now;
				
				System.Console.Out.Write(end.Millisecond - start.Millisecond);
				System.Console.Out.WriteLine(" total milliseconds");
			}
			catch (System.Exception e)
			{
				System.Console.Error.WriteLine(e.StackTrace);
			}
		}
		
		/* Walk directory hierarchy in uid order, while keeping uid iterator from
		/* existing index in sync.  Mismatches indicate one of: (a) old documents to
		/* be deleted; (b) unchanged documents, to be left alone; or (c) new
		/* documents, to be indexed.
		*/
		
		private static void  IndexDocs(System.IO.FileInfo file, System.IO.FileInfo index, bool create)
		{
			if (!create)
			{
				// incrementally update
				
				reader = IndexReader.Open(FSDirectory.Open(index), false); // open existing index
				uidIter = reader.Terms(new Term("uid", "")); // init uid iterator
				
				IndexDocs(file);
				
				if (deleting)
				{
					// delete rest of stale docs
					while (uidIter.Term() != null && (System.Object) uidIter.Term().Field() == (System.Object) "uid")
					{
						System.Console.Out.WriteLine("deleting " + HTMLDocument.Uid2url(uidIter.Term().Text()));
						reader.DeleteDocuments(uidIter.Term());
						uidIter.Next();
					}
					deleting = false;
				}
				
				uidIter.Close(); // close uid iterator
				reader.Close(); // close existing index
			}
			// don't have exisiting
			else
				IndexDocs(file);
		}
		
		private static void  IndexDocs(System.IO.FileInfo file)
		{
			if (System.IO.Directory.Exists(file.FullName))
			{
				// if a directory
				System.String[] files = System.IO.Directory.GetFileSystemEntries(file.FullName); // list its files
				System.Array.Sort(files); // sort the files
				for (int i = 0; i < files.Length; i++)
				// recursively index them
					IndexDocs(new System.IO.FileInfo(System.IO.Path.Combine(file.FullName, files[i])));
			}
			else if (file.FullName.EndsWith(".html") || file.FullName.EndsWith(".htm") || file.FullName.EndsWith(".txt"))
			{
				// index .txt files
				
				if (uidIter != null)
				{
					System.String uid = HTMLDocument.Uid(file); // construct uid for doc
					
					while (uidIter.Term() != null && (System.Object) uidIter.Term().Field() == (System.Object) "uid" && String.CompareOrdinal(uidIter.Term().Text(), uid) < 0)
					{
						if (deleting)
						{
							// delete stale docs
							System.Console.Out.WriteLine("deleting " + HTMLDocument.Uid2url(uidIter.Term().Text()));
							reader.DeleteDocuments(uidIter.Term());
						}
						uidIter.Next();
					}
					if (uidIter.Term() != null && (System.Object) uidIter.Term().Field() == (System.Object) "uid" && String.CompareOrdinal(uidIter.Term().Text(), uid) == 0)
					{
						uidIter.Next(); // keep matching docs
					}
					else if (!deleting)
					{
						// add new docs
						Document doc = HTMLDocument.Document(file);
						System.Console.Out.WriteLine("adding " + doc.Get("path"));
						writer.AddDocument(doc);
					}
				}
				else
				{
					// creating a new index
					Document doc = HTMLDocument.Document(file);
					System.Console.Out.WriteLine("adding " + doc.Get("path"));
					writer.AddDocument(doc); // add docs unconditionally
				}
			}
		}
	}
}