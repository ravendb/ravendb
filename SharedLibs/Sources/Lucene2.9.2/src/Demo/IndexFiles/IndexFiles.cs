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
using IndexWriter = Lucene.Net.Index.IndexWriter;
using FSDirectory = Lucene.Net.Store.FSDirectory;
using Version = Lucene.Net.Util.Version;

namespace Lucene.Net.Demo
{
	
	/// <summary>Index all text files under a directory. </summary>
	public class IndexFiles
	{
		
		private IndexFiles()
		{
		}
		
		internal static readonly System.IO.FileInfo INDEX_DIR = new System.IO.FileInfo("index");
		
		/// <summary>Index all text files under a directory. </summary>
		[STAThread]
		public static void  Main(System.String[] args)
		{
			System.String usage = typeof(IndexFiles) + " <root_directory>";
			if (args.Length == 0)
			{
				System.Console.Error.WriteLine("Usage: " + usage);
				System.Environment.Exit(1);
			}
			
			bool tmpBool;
			if (System.IO.File.Exists(INDEX_DIR.FullName))
				tmpBool = true;
			else
				tmpBool = System.IO.Directory.Exists(INDEX_DIR.FullName);
			if (tmpBool)
			{
				System.Console.Out.WriteLine("Cannot save index to '" + INDEX_DIR + "' directory, please delete it first");
				System.Environment.Exit(1);
			}
			
			System.IO.FileInfo docDir = new System.IO.FileInfo(args[0]);
			bool tmpBool2;
			if (System.IO.File.Exists(docDir.FullName))
				tmpBool2 = true;
			else
				tmpBool2 = System.IO.Directory.Exists(docDir.FullName);
			if (!tmpBool2) // || !docDir.canRead()) // {{Aroush}} what is canRead() in C#?
			{
				System.Console.Out.WriteLine("Document directory '" + docDir.FullName + "' does not exist or is not readable, please check the path");
				System.Environment.Exit(1);
			}
			
			System.DateTime start = System.DateTime.Now;
			try
			{
				IndexWriter writer = new IndexWriter(FSDirectory.Open(INDEX_DIR), new StandardAnalyzer(Version.LUCENE_CURRENT), true, IndexWriter.MaxFieldLength.LIMITED);
				System.Console.Out.WriteLine("Indexing to directory '" + INDEX_DIR + "'...");
				IndexDocs(writer, docDir);
				System.Console.Out.WriteLine("Optimizing...");
				writer.Optimize();
				writer.Close();
				
				System.DateTime end = System.DateTime.Now;
				System.Console.Out.WriteLine(end.Millisecond - start.Millisecond + " total milliseconds");
			}
			catch (System.IO.IOException e)
			{
				System.Console.Out.WriteLine(" caught a " + e.GetType() + "\n with message: " + e.Message);
			}
		}
		
		internal static void  IndexDocs(IndexWriter writer, System.IO.FileInfo file)
		{
			// do not try to index files that cannot be read
			// if (file.canRead())  // {{Aroush}} what is canRead() in C#?
			{
				if (System.IO.Directory.Exists(file.FullName))
				{
					System.String[] files = System.IO.Directory.GetFileSystemEntries(file.FullName);
					// an IO error could occur
					if (files != null)
					{
						for (int i = 0; i < files.Length; i++)
						{
							IndexDocs(writer, new System.IO.FileInfo(files[i]));
						}
					}
				}
				else
				{
					System.Console.Out.WriteLine("adding " + file);
					try
					{
						writer.AddDocument(FileDocument.Document(file));
					}
					// at least on windows, some temporary files raise this exception with an "access denied" message
					// checking if the file can be read doesn't help
					catch (System.IO.FileNotFoundException fnfe)
					{
						;
					}
				}
			}
		}
	}
}