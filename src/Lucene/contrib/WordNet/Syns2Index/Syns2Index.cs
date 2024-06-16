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
using Lucene.Net.Store;
using Analyzer = Lucene.Net.Analysis.Analyzer;
using Directory = System.IO.Directory;
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexWriter = Lucene.Net.Index.IndexWriter;

namespace WorldNet.Net
{
	
	/// <summary> Convert the prolog file wn_s.pl from the <a href="http://www.cogsci.princeton.edu/2.0/WNprolog-2.0.tar.gz">WordNet prolog download</a>
	/// into a Lucene index suitable for looking up synonyms and performing query expansion (<see cref="SynExpand.Expand"/>).
	/// 
	/// This has been tested with WordNet 2.0.
	/// 
	/// The index has fields named "word" (<see cref="F_WORD"/>)
	/// and "syn" (<see cref="F_SYN"/>).
	/// <p>
	/// The source word (such as 'big') can be looked up in the
	/// "word" field, and if present there will be fields named "syn"
	/// for every synonym. What's tricky here is that there could be <b>multiple</b>
	/// fields with the same name, in the general case for words that have multiple synonyms.
	/// That's not a problem with Lucene, you just use <see cref="Document.GetValues"/>
	/// </p>
	/// <p>
	/// While the WordNet file distinguishes groups of synonyms with
	/// related meanings we don't do that here.
	/// </p>
	/// This can take 4 minutes to execute and build an index on a "fast" system and the index takes up almost 3 MB.
	/// </summary>
	/// 
	/// <seealso cref="http://www.cogsci.princeton.edu/~wn/"></seealso>
	/// <seealso cref="http://www.cogsci.princeton.edu/~wn/man/prologdb.5WN.html"></seealso>
	/// <seealso cref="http://www.hostmon.com/rfc/advanced.jsp"> </seealso>
	public class Syns2Index
	{
		/// <summary> </summary>
		private static readonly System.IO.StreamWriter o;
		
		/// <summary> </summary>
		private static readonly System.IO.StreamWriter err;
		
		/// <summary> </summary>
		public const System.String F_SYN = "syn";
		
		/// <summary> </summary>
		public const System.String F_WORD = "word";
		
		/// <summary> </summary>
		private static readonly Analyzer ana = new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_CURRENT);
		
		/// <summary> 
		/// Takes arg of prolog file name and index directory.
		/// </summary>
		[STAThread]
		public static void  Main(System.String[] args)
		{
			// get command line arguments
			String prologFilename = null; // name of file "wn_s.pl"
			String indexDir = null;
			if (args.Length == 2)
			{
				prologFilename = args[0];
				indexDir = args[1];
			}
			else
			{
				Usage();
				Environment.Exit(1);
			}
			
			// ensure that the prolog file is readable
			if (!(new FileInfo(prologFilename)).Exists)
			{
				err.WriteLine("Error: cannot read Prolog file: " + prologFilename);
				Environment.Exit(1);
			}
			// exit if the target index directory already exists
			if (Directory.Exists((new FileInfo(indexDir)).FullName))
			{
				err.WriteLine("Error: index directory already exists: " + indexDir);
				err.WriteLine("Please specify a name of a non-existent directory");
				Environment.Exit(1);
			}
			
			o.WriteLine("Opening Prolog file " + prologFilename);
			var fis = new FileStream(prologFilename, FileMode.Open, FileAccess.Read);
			var br = new StreamReader(new StreamReader(fis, System.Text.Encoding.Default).BaseStream, new StreamReader(fis, System.Text.Encoding.Default).CurrentEncoding);
			String line;
			
			// maps a word to all the "groups" it's in
			System.Collections.IDictionary word2Nums = new System.Collections.SortedList();
			// maps a group to all the words in it
			System.Collections.IDictionary num2Words = new System.Collections.SortedList();
			// number of rejected words
			var ndecent = 0;
			
			// status output
			var mod = 1;
			var row = 1;
			// parse prolog file
			o.WriteLine("[1/2] Parsing " + prologFilename);
			while ((line = br.ReadLine()) != null)
			{
				// occasional progress
				if ((++row) % mod == 0) // periodically print out line we read in
				{
					mod *= 2;
					o.WriteLine("\t" + row + " " + line + " " + word2Nums.Count + " " + num2Words.Count + " ndecent=" + ndecent);
				}
				
				// syntax check
				if (!line.StartsWith("s("))
				{
					err.WriteLine("OUCH: " + line);
					Environment.Exit(1);
				}
				
				// parse line
				line = line.Substring(2);
				var comma = line.IndexOf(',');
				var num = line.Substring(0, comma);
				var q1 = line.IndexOf('\'');
				line = line.Substring(q1 + 1);
				var q2 = line.IndexOf('\'');
				var word = line.Substring(0, q2).ToLower().Replace("''", "'");
				
				// make sure is a normal word
				if (!IsDecent(word))
				{
					ndecent++;
					continue; // don't store words w/ spaces
				}
				
				// 1/2: word2Nums map
				// append to entry or add new one
				var lis = (System.Collections.IList) word2Nums[word];
				if (lis == null)
				{
					lis = new List<String> {num};
					word2Nums[word] = lis;
				}
				else
					lis.Add(num);
				
				// 2/2: num2Words map
				lis = (System.Collections.IList) num2Words[num];
				if (lis == null)
				{
					lis = new List<String> { word };
					num2Words[num] = lis;
				}
				else
					lis.Add(word);
			}
			
			// close the streams
			fis.Close();
			br.Close();
			
			// create the index
			o.WriteLine("[2/2] Building index to store synonyms, " + " map sizes are " + word2Nums.Count + " and " + num2Words.Count);
			Index(indexDir, word2Nums, num2Words);
		}
		
		/// <summary> 
		/// Checks to see if a word contains only alphabetic characters by
		/// checking it one character at a time.
		/// </summary>
		/// <param name="s">string to check </param>
		/// <returns> <c>true</c> if the string is decent</returns>
		private static bool IsDecent(String s)
		{
			var len = s.Length;
			for (var i = 0; i < len; i++)
			{
				if (!Char.IsLetter(s[i]))
				{
					return false;
				}
			}
			return true;
		}
		
		/// <summary> 
		/// Forms a Lucene index based on the 2 maps.
		/// </summary>
		/// <param name="indexDir">the direcotry where the index should be created</param>
		/// <param name="word2Nums">word2Nums</param>
		/// <param name="num2Words">num2Words</param>
		private static void  Index(String indexDir, System.Collections.IDictionary word2Nums, System.Collections.IDictionary num2Words)
		{
			var row = 0;
			var mod = 1;
			
			using (var dir = FSDirectory.Open(new DirectoryInfo(indexDir)))
			{
				var writer = new IndexWriter(dir, ana, true, IndexWriter.MaxFieldLength.LIMITED);
				writer.UseCompoundFile = true; // why?

				var i1 = word2Nums.Keys.GetEnumerator();
				while (i1.MoveNext())
				{
					var g = (String)i1.Current;
					var doc = new Document();

					var n = Index(word2Nums, num2Words, g, doc);
					if (n > 0)
					{
						doc.Add(new Field(F_WORD, g, Field.Store.YES, Field.Index.NOT_ANALYZED));
						if ((++row % mod) == 0)
						{
							o.WriteLine("\trow=" + row + "/" + word2Nums.Count + " doc= " + doc);
							mod *= 2;
						}
						writer.AddDocument(doc);
					}
				}
				o.WriteLine("Optimizing..");
				writer.Optimize();
				writer.Close();
			}
			
		}

		/// <summary> 
		/// Given the 2 maps fills a document for 1 word.
		/// </summary>
		private static int Index(System.Collections.IDictionary word2Nums, System.Collections.IDictionary num2Words, System.String g, Document doc)
		{
			var keys = (System.Collections.IList) word2Nums[g]; // get list of key#'s
			var i2 = keys.GetEnumerator();
			
			var already = new System.Collections.SortedList(); // keep them sorted
			
			// pass 1: fill up 'already' with all words
			while (i2.MoveNext()) // for each key#
			{
				foreach (var item in
					((System.Collections.IList) num2Words[i2.Current]).Cast<object>().Where(item => already.Contains(item) == false))
				{
					already.Add(item, item);
				}
			}

			var num = 0;
			already.Remove(g); // of course a word is it's own syn
			var it = already.GetEnumerator();
			while (it.MoveNext())
			{
				var cur = (String) it.Key;
				// don't store things like 'pit bull' -> 'american pit bull'
				if (!IsDecent(cur))
				{
					continue;
				}
				num++;
				doc.Add(new Field(F_SYN, cur, Field.Store.YES, Field.Index.NO));
			}
			return num;
		}
		
		/// <summary> </summary>
		private static void  Usage()
		{
			o.WriteLine("\n\n" + typeof(Syns2Index) + " <prolog file> <index dir>\n\n");
		}

	}
}