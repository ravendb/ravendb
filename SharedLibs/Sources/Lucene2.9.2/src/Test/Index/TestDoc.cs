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

using NUnit.Framework;

using SimpleAnalyzer = Lucene.Net.Analysis.SimpleAnalyzer;
using FileDocument = Lucene.Net.Demo.FileDocument;
using Document = Lucene.Net.Documents.Document;
using Directory = Lucene.Net.Store.Directory;
using FSDirectory = Lucene.Net.Store.FSDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
	
	/// <summary>JUnit adaptation of an older test case DocTest.
	/// 
	/// </summary>
	/// <version>  $Id: TestDoc.java 780770 2009-06-01 18:34:10Z uschindler $
	/// </version>
	[TestFixture]
	public class TestDoc:LuceneTestCase
	{
		
		/// <summary>Main for running test case by itself. </summary>
		[STAThread]
		public static void  Main(System.String[] args)
		{
			// TestRunner.run(new TestSuite(typeof(TestDoc))); // {{Aroush-2.9}} how is this done in NUnit?
		}
		
		
		private System.IO.FileInfo workDir;
		private System.IO.FileInfo indexDir;
		private System.Collections.ArrayList files;
		
		
		/// <summary>Set the test case. This test case needs
		/// a few text files created in the current working directory.
		/// </summary>
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			workDir = new System.IO.FileInfo(System.IO.Path.Combine(SupportClass.AppSettings.Get("tempDir", ""), "TestDoc"));
			System.IO.Directory.CreateDirectory(workDir.FullName);
			
			indexDir = new System.IO.FileInfo(System.IO.Path.Combine(workDir.FullName, "testIndex"));
			System.IO.Directory.CreateDirectory(indexDir.FullName);
			
			Directory directory = FSDirectory.Open(indexDir);
			directory.Close();
			
			files = new System.Collections.ArrayList();
			files.Add(CreateOutput("test.txt", "This is the first test file"));
			
			files.Add(CreateOutput("test2.txt", "This is the second test file"));
		}
		
		private System.IO.FileInfo CreateOutput(System.String name, System.String text)
		{
			System.IO.StreamWriter fw = null;
			System.IO.StreamWriter pw = null;
			
			try
			{
				System.IO.FileInfo f = new System.IO.FileInfo(System.IO.Path.Combine(workDir.FullName, name));
				bool tmpBool;
				if (System.IO.File.Exists(f.FullName))
					tmpBool = true;
				else
					tmpBool = System.IO.Directory.Exists(f.FullName);
				if (tmpBool)
				{
					bool tmpBool2;
					if (System.IO.File.Exists(f.FullName))
					{
						System.IO.File.Delete(f.FullName);
						tmpBool2 = true;
					}
					else if (System.IO.Directory.Exists(f.FullName))
					{
						System.IO.Directory.Delete(f.FullName);
						tmpBool2 = true;
					}
					else
						tmpBool2 = false;
					bool generatedAux = tmpBool2;
				}
				
				fw = new System.IO.StreamWriter(f.FullName, false, System.Text.Encoding.Default);
				pw = new System.IO.StreamWriter(fw.BaseStream, fw.Encoding);
				pw.WriteLine(text);
				return f;
			}
			finally
			{
				if (pw != null)
				{
					pw.Close();
				}
			}
		}
		
		
		/// <summary>This test executes a number of merges and compares the contents of
		/// the segments created when using compound file or not using one.
		/// 
		/// TODO: the original test used to print the segment contents to System.out
		/// for visual validation. To have the same effect, a new method
		/// checkSegment(String name, ...) should be created that would
		/// assert various things about the segment.
		/// </summary>
		[Test]
		public virtual void  TestIndexAndMerge()
		{
			System.IO.MemoryStream sw = new System.IO.MemoryStream();
			System.IO.StreamWriter out_Renamed = new System.IO.StreamWriter(sw);
			
			Directory directory = FSDirectory.Open(indexDir);
			IndexWriter writer = new IndexWriter(directory, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			SegmentInfo si1 = IndexDoc(writer, "test.txt");
			PrintSegment(out_Renamed, si1);
			
			SegmentInfo si2 = IndexDoc(writer, "test2.txt");
			PrintSegment(out_Renamed, si2);
			writer.Close();
			
			SegmentInfo siMerge = Merge(si1, si2, "merge", false);
			PrintSegment(out_Renamed, siMerge);
			
			SegmentInfo siMerge2 = Merge(si1, si2, "merge2", false);
			PrintSegment(out_Renamed, siMerge2);
			
			SegmentInfo siMerge3 = Merge(siMerge, siMerge2, "merge3", false);
			PrintSegment(out_Renamed, siMerge3);
			
			directory.Close();
			out_Renamed.Close();
			sw.Close();
			System.String multiFileOutput = System.Text.ASCIIEncoding.ASCII.GetString(sw.ToArray());
			//System.out.println(multiFileOutput);
			
			sw = new System.IO.MemoryStream();
			out_Renamed = new System.IO.StreamWriter(sw);
			
			directory = FSDirectory.Open(indexDir);
			writer = new IndexWriter(directory, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			si1 = IndexDoc(writer, "test.txt");
			PrintSegment(out_Renamed, si1);
			
			si2 = IndexDoc(writer, "test2.txt");
			PrintSegment(out_Renamed, si2);
			writer.Close();
			
			siMerge = Merge(si1, si2, "merge", true);
			PrintSegment(out_Renamed, siMerge);
			
			siMerge2 = Merge(si1, si2, "merge2", true);
			PrintSegment(out_Renamed, siMerge2);
			
			siMerge3 = Merge(siMerge, siMerge2, "merge3", true);
			PrintSegment(out_Renamed, siMerge3);
			
			directory.Close();
			out_Renamed.Close();
			sw.Close();
			System.String singleFileOutput = System.Text.ASCIIEncoding.ASCII.GetString(sw.ToArray());
			
			Assert.AreEqual(multiFileOutput, singleFileOutput);
		}
		
		private SegmentInfo IndexDoc(IndexWriter writer, System.String fileName)
		{
			System.IO.FileInfo file = new System.IO.FileInfo(System.IO.Path.Combine(workDir.FullName, fileName));
			Document doc = FileDocument.Document(file);
			writer.AddDocument(doc);
			writer.Flush();
			return writer.NewestSegment();
		}
		
		
		private SegmentInfo Merge(SegmentInfo si1, SegmentInfo si2, System.String merged, bool useCompoundFile)
		{
			SegmentReader r1 = SegmentReader.Get(si1);
			SegmentReader r2 = SegmentReader.Get(si2);
			
			SegmentMerger merger = new SegmentMerger(si1.dir, merged);
			
			merger.Add(r1);
			merger.Add(r2);
			merger.Merge();
			merger.CloseReaders();
			
			if (useCompoundFile)
			{
				System.Collections.IList filesToDelete = merger.CreateCompoundFile(merged + ".cfs");
				for (System.Collections.IEnumerator iter = filesToDelete.GetEnumerator(); iter.MoveNext(); )
				{
					si1.dir.DeleteFile((System.String) iter.Current);
				}
			}
			
			return new SegmentInfo(merged, si1.docCount + si2.docCount, si1.dir, useCompoundFile, true);
		}
		
		
		private void  PrintSegment(System.IO.StreamWriter out_Renamed, SegmentInfo si)
		{
			SegmentReader reader = SegmentReader.Get(si);
			
			for (int i = 0; i < reader.NumDocs(); i++)
			{
				out_Renamed.WriteLine(reader.Document(i));
			}
			
			TermEnum tis = reader.Terms();
			while (tis.Next())
			{
				out_Renamed.Write(tis.Term());
				out_Renamed.WriteLine(" DF=" + tis.DocFreq());
				
				TermPositions positions = reader.TermPositions(tis.Term());
				try
				{
					while (positions.Next())
					{
						out_Renamed.Write(" doc=" + positions.Doc());
						out_Renamed.Write(" TF=" + positions.Freq());
						out_Renamed.Write(" pos=");
						out_Renamed.Write(positions.NextPosition());
						for (int j = 1; j < positions.Freq(); j++)
							out_Renamed.Write("," + positions.NextPosition());
						out_Renamed.WriteLine("");
					}
				}
				finally
				{
					positions.Close();
				}
			}
			tis.Close();
			reader.Close();
		}
	}
}