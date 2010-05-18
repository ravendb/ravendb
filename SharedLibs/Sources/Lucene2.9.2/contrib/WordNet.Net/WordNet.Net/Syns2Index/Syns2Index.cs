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
using Field = Lucene.Net.Documents.Field;
using IndexWriter = Lucene.Net.Index.IndexWriter;

namespace WorldNet.Net
{
	
	/// <summary> Convert the prolog file wn_s.pl from the <a href="http://www.cogsci.princeton.edu/2.0/WNprolog-2.0.tar.gz">WordNet prolog download</a>
	/// into a Lucene index suitable for looking up synonyms and performing query expansion ({@link SynExpand#expand SynExpand.expand(...)}).
	/// 
	/// This has been tested with WordNet 2.0.
	/// 
	/// The index has fields named "word" ({@link #F_WORD})
	/// and "syn" ({@link #F_SYN}).
	/// <p>
	/// The source word (such as 'big') can be looked up in the
	/// "word" field, and if present there will be fields named "syn"
	/// for every synonym. What's tricky here is that there could be <b>multiple</b>
	/// fields with the same name, in the general case for words that have multiple synonyms.
	/// That's not a problem with Lucene, you just use {@link org.apache.lucene.document.Document#getValues}
	/// </p>
	/// <p>
	/// While the WordNet file distinguishes groups of synonyms with
	/// related meanings we don't do that here.
	/// </p>
	/// 
	/// This can take 4 minutes to execute and build an index on a "fast" system and the index takes up almost 3 MB.
	/// 
	/// </summary>
	/// <author>  Dave Spencer, dave&#064;searchmorph.com
	/// </author>
	/// <seealso cref="href="http://www.cogsci.princeton.edu/~wn/">WordNet home page</a>">
	/// </seealso>
	/// <seealso cref="href="http://www.cogsci.princeton.edu/~wn/man/prologdb.5WN.html">prologdb man page</a>">
	/// </seealso>
	/// <seealso cref="href="http://www.hostmon.com/rfc/advanced.jsp">sample site that uses it</a>">
	/// </seealso>
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
		private static readonly Analyzer ana = new StandardAnalyzer();
		
		/// <summary> Takes arg of prolog file name and index directory.</summary>
		[STAThread]
		public static void  Main(System.String[] args)
		{
			// get command line arguments
			System.String prologFilename = null; // name of file "wn_s.pl"
			System.String indexDir = null;
			if (args.Length == 2)
			{
				prologFilename = args[0];
				indexDir = args[1];
			}
			else
			{
				Usage();
				System.Environment.Exit(1);
			}
			
			// ensure that the prolog file is readable
			if (!(new System.IO.FileInfo(prologFilename)).Exists)
			{
				err.WriteLine("Error: cannot read Prolog file: " + prologFilename);
				System.Environment.Exit(1);
			}
			// exit if the target index directory already exists
			if (System.IO.Directory.Exists((new System.IO.FileInfo(indexDir)).FullName))
			{
				err.WriteLine("Error: index directory already exists: " + indexDir);
				err.WriteLine("Please specify a name of a non-existent directory");
				System.Environment.Exit(1);
			}
			
			o.WriteLine("Opening Prolog file " + prologFilename);
			System.IO.FileStream fis = new System.IO.FileStream(prologFilename, System.IO.FileMode.Open, System.IO.FileAccess.Read);
			System.IO.StreamReader br = new System.IO.StreamReader(new System.IO.StreamReader(fis, System.Text.Encoding.Default).BaseStream, new System.IO.StreamReader(fis, System.Text.Encoding.Default).CurrentEncoding);
			System.String line;
			
			// maps a word to all the "groups" it's in
			System.Collections.IDictionary word2Nums = new System.Collections.SortedList();
			// maps a group to all the words in it
			System.Collections.IDictionary num2Words = new System.Collections.SortedList();
			// number of rejected words
			int ndecent = 0;
			
			// status output
			int mod = 1;
			int row = 1;
			// parse prolog file
			o.WriteLine("[1/2] Parsing " + prologFilename);
			while ((line = br.ReadLine()) != null)
			{
				// occasional progress
				if ((++row) % mod == 0)
				// periodically print out line we read in
				{
					mod *= 2;
					o.WriteLine("\t" + row + " " + line + " " + word2Nums.Count + " " + num2Words.Count + " ndecent=" + ndecent);
				}
				
				// syntax check
				if (!line.StartsWith("s("))
				{
					err.WriteLine("OUCH: " + line);
					System.Environment.Exit(1);
				}
				
				// parse line
				line = line.Substring(2);
				int comma = line.IndexOf((System.Char) ',');
				System.String num = line.Substring(0, (comma) - (0));
				int q1 = line.IndexOf((System.Char) '\'');
				line = line.Substring(q1 + 1);
				int q2 = line.IndexOf((System.Char) '\'');
				System.String word = line.Substring(0, (q2) - (0)).ToLower();
				
				// make sure is a normal word
				if (!IsDecent(word))
				{
					ndecent++;
					continue; // don't store words w/ spaces
				}
				
				// 1/2: word2Nums map
				// append to entry or add new one
				System.Collections.IList lis = (System.Collections.IList) word2Nums[word];
				if (lis == null)
				{
					lis = new System.Collections.ArrayList();
					lis.Add(num);
					word2Nums[word] = lis;
				}
				else
					lis.Add(num);
				
				// 2/2: num2Words map
				lis = (System.Collections.IList) num2Words[num];
				if (lis == null)
				{
					lis = new System.Collections.ArrayList();
					lis.Add(word);
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
		
		/// <summary> Checks to see if a word contains only alphabetic characters by
		/// checking it one character at a time.
		/// 
		/// </summary>
		/// <param name="s">string to check
		/// </param>
		/// <returns> <code>true</code> if the string is decent
		/// </returns>
		private static bool IsDecent(System.String s)
		{
			int len = s.Length;
			for (int i = 0; i < len; i++)
			{
				if (!System.Char.IsLetter(s[i]))
				{
					return false;
				}
			}
			return true;
		}
		
		/// <summary> Forms a Lucene index based on the 2 maps.
		/// 
		/// </summary>
		/// <param name="indexDir">the direcotry where the index should be created
		/// </param>
		/// <param name="">word2Nums
		/// </param>
		/// <param name="">num2Words
		/// </param>
		private static void  Index(System.String indexDir, System.Collections.IDictionary word2Nums, System.Collections.IDictionary num2Words)
		{
			int row = 0;
			int mod = 1;
			
			// override the specific index if it already exists
			IndexWriter writer = new IndexWriter(indexDir, ana, true);
			writer.SetUseCompoundFile(true); // why?
			// blindly up these parameters for speed
			writer.SetMergeFactor(writer.GetMergeFactor() * 2);
			writer.SetMaxBufferedDocs(writer.GetMaxBufferedDocs() * 2);
			System.Collections.IEnumerator i1 = word2Nums.Keys.GetEnumerator();
			while (i1.MoveNext())
			// for each word
			{
				System.String g = (System.String) i1.Current;
				Document doc = new Document();
				
				int n = Index(word2Nums, num2Words, g, doc);
				if (n > 0)
				{
					doc.Add(new Field(F_WORD, g, Field.Store.YES, Field.Index.UN_TOKENIZED));
					if ((++row % mod) == 0)
					{
						o.WriteLine("\trow=" + row + "/" + word2Nums.Count + " doc= " + doc);
						mod *= 2;
					}
					writer.AddDocument(doc);
				} // else degenerate
			}
			o.WriteLine("Optimizing..");
			writer.Optimize();
			writer.Close();
		}

		/// <summary> Given the 2 maps fills a document for 1 word.</summary>
		private static int Index(System.Collections.IDictionary word2Nums, System.Collections.IDictionary num2Words, System.String g, Document doc)
		{
			System.Collections.IList keys = (System.Collections.IList) word2Nums[g]; // get list of key#'s
			System.Collections.IEnumerator i2 = keys.GetEnumerator();
			
			System.Collections.SortedList already = new System.Collections.SortedList(); // keep them sorted
			
			// pass 1: fill up 'already' with all words
			while (i2.MoveNext()) // for each key#
			{
				foreach (object item in (System.Collections.IList) num2Words[i2.Current]) // get list of words
				{
					if (already.Contains(item) == false)
					{
						already.Add(item, item); 
					}
				}
			}
			int num = 0;
			already.Remove(g); // of course a word is it's own syn
			System.Collections.IDictionaryEnumerator it = already.GetEnumerator();
			while (it.MoveNext())
			{
				System.String cur = (System.String) it.Key;
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

		static Syns2Index()
		{
			System.IO.StreamWriter temp_writer;
			temp_writer = new System.IO.StreamWriter(System.Console.OpenStandardOutput(), System.Console.Out.Encoding);
			temp_writer.AutoFlush = true;
			o = temp_writer;
			System.IO.StreamWriter temp_writer2;
			temp_writer2 = new System.IO.StreamWriter(System.Console.OpenStandardError(), System.Console.Error.Encoding);
			temp_writer2.AutoFlush = true;
			err = temp_writer2;
		}
	}
}